package org.sunbird.dp.contentUpdate.domain

import org.sunbird.dp.core.domain.{Events, EventsPath}

import java.util

class Event(eventMap: util.Map[String, Any]) extends Events(eventMap) {
  private val jobName = "contentUpdateEvent"

  def identifier:String={
    telemetry.read[String]("identifier").get
  }

  def averageRatingScore: Double = {
    telemetry.read[Double]("averageRatingScore").get
  }

  def totalRatingsCount: Double = {
    telemetry.read[Double]("totalRatingsCount").get
  }

  def markFailed(errorMsg: String): Unit = {
    telemetry.addFieldIfAbsent(EventsPath.FLAGS_PATH, new util.HashMap[String, Boolean])
    telemetry.addFieldIfAbsent("metadata", new util.HashMap[String, AnyRef])
    telemetry.add("metadata.validation_error", errorMsg)
    telemetry.add("metadata.src", jobName)
  }
}
