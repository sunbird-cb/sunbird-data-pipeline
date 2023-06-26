package org.sunbird.assessment.submit.domain

import org.sunbird.dp.core.domain.{Events, EventsPath}

import java.util

class Event(eventMap: util.Map[String, Any]) extends Events(eventMap) {
  private val jobName = "assessmentFeatureJob"

  def contentId: String = {
    telemetry.read[String]("contentId").getOrElse("")
  }

  def courseId: String = {
    telemetry.read[String]("courseId").getOrElse("")
  }

  def batchId: String = {
    telemetry.read[String]("batchId").getOrElse("")
  }

  def userId: String = {
    telemetry.read[String]("userId").getOrElse("")
  }

  def assessmentId: String = {
    telemetry.read[String]("assessmentId").getOrElse("")
  }

  def primaryCategory: String = {
    telemetry.read[String]("primaryCategory").getOrElse("")
  }

  def competency: util.Map[String, String] = {
    telemetry.read[util.Map[String, String]]("competency").getOrElse(null)
  }

  def totalScore: Double = {
    telemetry.read[Double]("totalScore").getOrElse(0.0)
  }

  def passPercentage: Double = {
    telemetry.read[Double]("passPercentage").getOrElse(0.0)
  }

  def actor: util.Map[String, String] = {
    telemetry.read[util.Map[String, String]]("actor").orNull
  }

  def edata: util.Map[String, String] = {
    telemetry.read[util.Map[String, String]]("edata").orNull
  }

  def markFailed(errorMsg: String): Unit = {
    telemetry.addFieldIfAbsent(EventsPath.FLAGS_PATH, new util.HashMap[String, Boolean])
    telemetry.addFieldIfAbsent("metadata", new util.HashMap[String, AnyRef])
    telemetry.add("metadata.validation_error", errorMsg)
    telemetry.add("metadata.src", jobName)
  }

}
