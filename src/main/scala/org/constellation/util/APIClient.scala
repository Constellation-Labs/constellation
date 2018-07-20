package org.constellation.util

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshal}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import org.constellation.primitives.Schema.Id
import org.json4s.native.Serialization
import org.json4s.{Formats, native}

import scala.concurrent.{ExecutionContextExecutor, Future}

// TODO : Implement all methods from RPCInterface here for a client SDK
// This should also probably use scalaj http because it's bettermore standard.

class APIClient(val host: String = "127.0.0.1", val port: Int)(
  implicit val system: ActorSystem,
  implicit val materialize: ActorMaterializer,
  implicit val executionContext: ExecutionContextExecutor
) {

  var udpPort: Int = 16180
  var id: Id = null

  def udpAddress: String = host + ":" + udpPort

  def setExternalIP(): Boolean = postSync("ip", host + ":" + udpPort).status == StatusCodes.OK

  val baseURI = s"http://$host:$port"

  def base(suffix: String) = Uri(s"$baseURI/$suffix")

  val config = ConfigFactory.load()

  val authId = config.getString("auth.id")
  val authPassword = config.getString("auth.password")

  val authorization = headers.Authorization(BasicHttpCredentials(authId, authPassword))

  def get(suffix: String, queryParams: Map[String,String] = Map()): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(headers = List(authorization), uri = base(suffix).withQuery(Query(queryParams)))
    )
  }

  def getSync(suffix: String, queryParams: Map[String,String] = Map()): HttpResponse = {
    import constellation._
    Http().singleRequest(
      HttpRequest(headers = List(authorization), uri = base(suffix).withQuery(Query(queryParams)))
    ).get()
  }

  def addPeer(remote: String): HttpResponse = postSync("peer", remote)

  def getBlocking [T <: AnyRef](suffix: String, queryParams: Map[String,String] = Map(), timeout: Int = 5)
                               (implicit m : Manifest[T], f : Formats = constellation.constellationFormats): T = {
    import constellation.EasyFutureBlock
    val httpResponse = Http().singleRequest(
      HttpRequest(headers = List(authorization), uri = base(suffix).withQuery(Query(queryParams)))
    ).get(timeout)
    Unmarshal(httpResponse.entity).to[String].map { r => Serialization.read[T](r) }.get()
  }

  def getBlockingStr[T <: AnyRef](suffix: String, queryParams: Map[String,String] = Map(), timeout: Int = 5)
                                 (implicit m : Manifest[T], f : Formats = constellation.constellationFormats): String = {
    import constellation.EasyFutureBlock
    val httpResponse = Http().singleRequest(
      HttpRequest(headers = List(authorization), uri = base(suffix).withQuery(Query(queryParams)))
    ).get(timeout)
    Unmarshal(httpResponse.entity).to[String].get()
  }

  def post(suffix: String, t: AnyRef)(implicit f : Formats = constellation.constellationFormats): Future[HttpResponse] = {

    val ser = Serialization.write(t)
    Http().singleRequest(
      HttpRequest(headers = List(authorization), uri = base(suffix), method = HttpMethods.POST, entity = HttpEntity(
        ContentTypes.`application/json`, ser)
      ))
  }

  def postSync[T <: AnyRef](suffix: String, t: T)(
    implicit f : Formats = constellation.constellationFormats
  ): HttpResponse = {
    import constellation.EasyFutureBlock
    post(suffix, t).get()
  }

  implicit val serialization: Serialization.type = native.Serialization
  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] = PredefinedFromEntityUnmarshallers.stringUnmarshaller

  def read[T <: AnyRef](httpResponse: HttpResponse)(
    implicit m : Manifest[T], f : Formats = constellation.constellationFormats
  ): Future[T] =
    Unmarshal(httpResponse.entity).to[String].map{r => Serialization.read[T](r)}

  def postRead[Q <: AnyRef](suffix: String, t: AnyRef, timeout: Int = 5)(
    implicit m : Manifest[Q], f : Formats = constellation.constellationFormats
  ): Q = {
    import constellation.EasyFutureBlock

    read[Q](post(suffix, t).get(timeout)).get()
  }

}