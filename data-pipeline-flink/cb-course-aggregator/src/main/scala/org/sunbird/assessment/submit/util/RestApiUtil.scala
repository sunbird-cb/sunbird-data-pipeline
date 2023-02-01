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
  logger.info("RestApi Call start")

  def post(uri: String, params: java.util.Map[String, Any]): String = {
    try {
      val post = new HttpPost(uri)
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
      val jsonString = mapper.writeValueAsString(params)
      post.setHeader("Content-type", "application/json")
      post.setEntity(new StringEntity(jsonString))
      val client = new DefaultHttpClient()
      val response = client.execute(post)
      val statusCode = response.getStatusLine.getStatusCode
      var text = new String()
      if (statusCode.equals(200)) {
        logger.info("Rest Call successfully working")
        val myObjectMapper = new ObjectMapper()
        myObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        myObjectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        text = new String(IOUtils.toByteArray(response.getEntity.getContent), StandardCharsets.UTF_8)
      }
      text
    } catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Failed during performing post mode request %s", e.getMessage()))
        null
    }
  }

  def postRequestWithContentType(uri: String, params: java.util.Map[String, Any]): String = {
    val post = new HttpPost(uri)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
    val jsonString = mapper.writeValueAsString(params)
    logger.info("json String " + jsonString)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(jsonString))
    val client = new DefaultHttpClient()
    val response = client.execute(post)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode.equals(200)) {
      logger.info("Rest Call successfully working")
    }
    val text = new String(IOUtils.toByteArray(response.getEntity.getContent), StandardCharsets.UTF_8);
    text
  }
}