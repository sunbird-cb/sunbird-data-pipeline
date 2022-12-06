package org.sunbird.dp.rating.domain

import java.sql.Timestamp
import java.util

case class Response(id: String = "api.questionset.hierarchy.get",
                      ver: String = "1.0",
                      ts: String = new Timestamp(System.currentTimeMillis()).toString,
                      params: SunbirdApiRespParam = new SunbirdApiRespParam(),
                      responseCode: Int = 0,
                      result: util.Map[String, Any])

