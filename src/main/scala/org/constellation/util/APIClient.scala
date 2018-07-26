package org.constellation.util

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import org.constellation.primitives.Schema.Id
import org.json4s.{Formats, native}
import org.json4s.native.Serialization
import scalaj.http.{Http, HttpRequest, HttpResponse}

import scala.concurrent.Future

class APIClient(val host: String = "127.0.0.1", val port: Int) {

  import scala.concurrent.ExecutionContext.Implicits.global

  var udpPort: Int = 16180
  var id: Id = _
  var peerHttpPort: Int = _

  private val baseURI = s"http://$host:$port"

  def base(suffix: String) = s"$baseURI/$suffix"

  private val config = ConfigFactory.load()

  private val authId = config.getString("auth.id")
  private val authPassword = config.getString("auth.password")

  private val authEnabled = false

  implicit class HttpRequestAuth(req: HttpRequest) {
    def addAuthIfEnabled(): HttpRequest = {
      if (authEnabled) {
        req.auth(authId, authPassword)
      } else req
    }
  }

  def timeoutMS(timeoutSeconds: Int): Int = {
    TimeUnit.SECONDS.toMillis(timeoutSeconds).toInt
  }

  def httpWithAuth(suffix: String, timeoutSeconds: Int = 20): HttpRequest = {
    val timeoutMs = timeoutMS(timeoutSeconds)
    Http(base(suffix)).addAuthIfEnabled().timeout(timeoutMs, timeoutMs)
  }

  implicit val serialization: Serialization.type = native.Serialization

  def post(suffix: String, b: AnyRef, timeoutSeconds: Int = 5)(implicit f : Formats = constellation.constellationFormats): Future[HttpResponse[String]] = {
    Future(postSync(suffix, b))
  }

  def postEmpty(suffix: String, timeoutSeconds: Int = 5)(implicit f : Formats = constellation.constellationFormats)
  : HttpResponse[String] = {
    httpWithAuth(suffix).method("POST") .asString
  }

  def postSync(suffix: String, b: AnyRef, timeoutSeconds: Int = 5)(
    implicit f : Formats = constellation.constellationFormats
  ): HttpResponse[String] = {
    val ser = Serialization.write(b)
    httpWithAuth(suffix).postData(ser).header("content-type", "application/json").asString
  }

  def postBlocking[T <: AnyRef](suffix: String, b: AnyRef, timeoutSeconds: Int = 5)(implicit m : Manifest[T], f : Formats = constellation.constellationFormats): T = {
    val res = postSync(suffix, b)
    Serialization.read[T](res.body)
  }

  def postBlockingEmpty[T <: AnyRef](suffix: String, timeoutSeconds: Int = 5)(implicit m : Manifest[T], f : Formats = constellation.constellationFormats): T = {
    val res = postEmpty(suffix)
    Serialization.read[T](res.body)
  }

  def get(suffix: String, queryParams: Map[String,String] = Map(), timeoutSeconds: Int = 5): Future[HttpResponse[String]] = {
    Future(getSync(suffix, queryParams))
  }

  def getSync(suffix: String, queryParams: Map[String,String] = Map(), timeoutSeconds: Int = 5): HttpResponse[String] = {
    val req = httpWithAuth(suffix).params(queryParams)
    req.asString
  }

  def getBlocking[T <: AnyRef](suffix: String, queryParams: Map[String,String] = Map(), timeoutSeconds: Int = 5)
                              (implicit m : Manifest[T], f : Formats = constellation.constellationFormats): T = {
    Serialization.read[T](getBlockingStr(suffix, queryParams, timeoutSeconds))
  }

  def getBlockingStr(suffix: String, queryParams: Map[String,String] = Map(), timeoutSeconds: Int = 5): String = {
    val resp = httpWithAuth(suffix, timeoutSeconds).params(queryParams).asString
    resp.body
  }

}
