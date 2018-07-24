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
import org.constellation.serializer.KryoSerializer
import org.json4s.JsonAST.JArray
import org.json4s.native.Serialization
import org.json4s.{Formats, native}

import scala.concurrent.{ExecutionContextExecutor, Future}

// TODO : Implement all methods from RPCInterface here for a client SDK
// This should also probably use scalaj http because it's bettermore standard.

class APIClient (
  implicit val system: ActorSystem,
  implicit val executionContext: ExecutionContextExecutor,
  implicit val materialize: ActorMaterializer) {

  var hostName: String = "127.0.0.1"
  var id: Id = _

  var udpPort: Int = 16180
  var apiPort: Int = _

  def setConnection(host: String = "127.0.0.1", port: Int): APIClient = {
    hostName = host
    apiPort = port
    this
  }

  def udpAddress: String = hostName + ":" + udpPort

  def setExternalIP(): Boolean = postSync("ip", hostName + ":" + udpPort).status == StatusCodes.OK

  def baseURI = {
    s"http://$hostName:$apiPort"
  }

  def base(suffix: String) = Uri(s"$baseURI/$suffix")

  // val config = ConfigFactory.load()

  val authId = "dev"  // config.getString("auth.id")
  val authPassword = "p4ssw0rd" //config.getString("auth.password")

  val authorization = headers.Authorization(BasicHttpCredentials(authId, authPassword))

  private val authHeaders = List() //authorization)

  def get(suffix: String, queryParams: Map[String,String] = Map()): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(headers = authHeaders, uri = base(suffix).withQuery(Query(queryParams)))
    )
  }

  def getSync(suffix: String, queryParams: Map[String,String] = Map()): HttpResponse = {
    import constellation._
    Http().singleRequest(
      HttpRequest(headers = authHeaders, uri = base(suffix).withQuery(Query(queryParams)))
    ).get()
  }

  def addPeer(remote: String): HttpResponse = postSync("peer", remote)

  def getBlocking [T <: AnyRef](suffix: String, queryParams: Map[String,String] = Map(), timeout: Int = 5)
                               (implicit m : Manifest[T], f : Formats = constellation.constellationFormats): T = {
    import constellation.EasyFutureBlock
    val httpResponse = Http().singleRequest(
      HttpRequest(headers = authHeaders, uri = base(suffix).withQuery(Query(queryParams)))
    ).get(timeout)
    Unmarshal(httpResponse.entity).to[String].map { r => Serialization.read[T](r) }.get()
  }

  def getBlockingStr[T <: AnyRef](suffix: String, queryParams: Map[String,String] = Map(), timeout: Int = 5)
                                 (implicit m : Manifest[T], f : Formats = constellation.constellationFormats): String = {
    import constellation.EasyFutureBlock
    val httpResponse = Http().singleRequest(
      HttpRequest(headers = authHeaders, uri = base(suffix).withQuery(Query(queryParams)))
    ).get(timeout)
    Unmarshal(httpResponse.entity).to[String].get()
  }

  def post(suffix: String, t: AnyRef)(implicit f : Formats = constellation.constellationFormats): Future[HttpResponse] = {

    val ser = Serialization.write(t)
    Http().singleRequest(
      HttpRequest(headers = authHeaders, uri = base(suffix), method = HttpMethods.POST, entity = HttpEntity(
        ContentTypes.`application/json`, ser)
      ))
  }

  def postEmpty(suffix: String)(implicit f : Formats = constellation.constellationFormats): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(headers = authHeaders, uri = base(suffix), method = HttpMethods.POST
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

  def postEmptyRead[Q <: AnyRef](suffix: String, timeout: Int = 5)(
    implicit m : Manifest[Q], f : Formats = constellation.constellationFormats
  ): Q = {
    import constellation.EasyFutureBlock

    read[Q](postEmpty(suffix).get(timeout)).get()
  }

}