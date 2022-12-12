package org.sunbird.dp.contentUpdate.util

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.apache.http.client.methods.{HttpPatch, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets

class RestApiUtil extends Serializable {


  private[this] val logger = LoggerFactory.getLogger(classOf[RestApiUtil])
  logger.info("RestApi Call start")

  def postRequest(uri: String, params: java.util.Map[String, Any]): String = {
    logger.info("Entering Post mode Rest Call")
    val post = new HttpPost(uri)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
    val jsonString = mapper.writeValueAsString(params)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(jsonString))
    val client = new DefaultHttpClient()
    val response = client.execute(post)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode.equals(200)) {
      logger.info("Http Post Mode Call successful")
    }
    val myObjectMapper = new ObjectMapper()
    myObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    myObjectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    val text = new String(response.getEntity.getContent.readAllBytes(), StandardCharsets.UTF_8);
    text
  }

  def patchRequest(uri: String, params: java.util.Map[String, Any]): Int = {
    logger.info("Entering Patch mode Rest call")
    logger.info("uri " + uri)
    val patch = new HttpPatch(uri)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
    val jsonString = mapper.writeValueAsString(params)
    patch.setHeader("Content-type", "application/json")
    patch.setEntity(new StringEntity(jsonString))
    val client = new DefaultHttpClient()
    val response = client.execute(patch)
    val statusCode = response.getStatusLine.getStatusCode
    logger.info("response " + response.toString)
    logger.info("statusCode " + statusCode)
    if (statusCode.equals(200)) {
      logger.info("Http Patch Mode Call successful")
    }
    statusCode
  }
}