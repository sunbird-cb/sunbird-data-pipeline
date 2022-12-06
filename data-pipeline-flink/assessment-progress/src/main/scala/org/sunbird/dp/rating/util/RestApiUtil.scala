package org.sunbird.dp.rating.util

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.http.client.methods.{HttpGet, HttpPatch, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets
import java.util

class RestApiUtil extends Serializable {


  private[this] val logger = LoggerFactory.getLogger(classOf[RestApiUtil])
  logger.info("RestApi Call start")

  def getRequest(uri: String, headersValue: util.HashMap[String, Any]): String = {
    val get = new HttpGet(uri)
    get.setHeader("x-authenticated-user-token", headersValue.get("x-authenticated-user-token").toString)
    get.setHeader("authorization", headersValue.get("authorization").toString)
    /*  if(!MapUtils.isEmpty(headersValue)){
      headersValue.forEach((k,v)=>{
        post.setHeader(k,v.toString)
      })
    }*/
    logger.info("heardes " + get.getURI + " " + get.getHeaders("authorization"))
    val client = new DefaultHttpClient()
    val response = client.execute(get)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode.equals(200)) {
      logger.info("Rest Call successfully working")
    }
    val text = new String(response.getEntity.getContent.readAllBytes(), StandardCharsets.UTF_8);
    logger.info("text " + text)
    text
  }

  def postRequest(uri: String, headersValue: util.HashMap[String, Any],params: util.HashMap[String, Any]): String = {
    val post = new HttpPost(uri)
      if(!MapUtils.isEmpty(headersValue)){
      headersValue.forEach((k,v)=>{
        post.setHeader(k,v.toString)
      })
    }
    logger.info("heardes " + post.getURI + " " + post.getHeaders("authorization"))
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
    val jsonString = mapper.writeValueAsString(params)
    post.setEntity(new StringEntity(jsonString))
    val client = new DefaultHttpClient()
    val response = client.execute(post)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode.equals(200)) {
      logger.info("Rest Call successfully working")
    }
    val text = new String(response.getEntity.getContent.readAllBytes(), StandardCharsets.UTF_8);
    logger.info("text " + text)
    text
  }
}