package org.sunbird.assessment.submit.util

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets


class RestApiUtil extends Serializable {

  private[this] val logger = LoggerFactory.getLogger(classOf[RestApiUtil])

  def post(uri: String, params: java.util.Map[String, Any]): String = {
    try {
      val post = new HttpPost(uri)
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
      post.setHeader("Content-type", "application/json")
      post.setEntity(new StringEntity(mapper.writeValueAsString(params)))
      val client = new DefaultHttpClient()
      val response = client.execute(post)
      val statusCode = response.getStatusLine.getStatusCode
      var text = new String()
      if (statusCode.equals(200)) {
        text = new String(IOUtils.toByteArray(response.getEntity.getContent), StandardCharsets.UTF_8)
      }
      text
    } catch {
      case e: Exception => e.printStackTrace()
        logger.error(String.format("Failed to make rest API call %s", e.getMessage()),e)
        null
    }
  }
}