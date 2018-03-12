package org.constellation.rpc

import java.security.PublicKey

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshal}
import akka.stream.ActorMaterializer
import org.constellation.blockchain.Transaction
import org.json4s.{DefaultFormats, Formats, native}
import org.json4s.native.Serialization

import scala.concurrent.{ExecutionContextExecutor, Future}


// TODO : Implement all methods from RPCInterface here for a client SDK
// This should also probably use scalaj http because it's bettermore standard.

class RPCClient(host: String = "127.0.0.1", port: Int)(
implicit val system: ActorSystem,
implicit val materialize: ActorMaterializer,
implicit val executionContext: ExecutionContextExecutor
) {

  val baseURI = s"http://$host:$port"

  def base(suffix: String) = Uri(s"$baseURI/$suffix")

  def query(suffix: String, queryParams: Map[String,String] = Map()): Future[HttpResponse] = {
  Http().singleRequest(
  HttpRequest(uri = base(suffix).withQuery(Query(queryParams)))
  )
}

  def post[T <: AnyRef](suffix: String, t: T)(implicit f : Formats = constellation.constellationFormats): Future[HttpResponse] = {
  val ser = Serialization.write(t)
  Http().singleRequest(
  HttpRequest(uri = base(suffix), method = HttpMethods.POST, entity = HttpEntity(
  ContentTypes.`application/json`, ser)
  ))
}

  implicit val serialization: Serialization.type = native.Serialization
  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] = PredefinedFromEntityUnmarshallers.stringUnmarshaller

  def read[T <: AnyRef](httpResponse: HttpResponse)(
  implicit m : Manifest[T], f : Formats = constellation.constellationFormats
  ): Future[T] =
  Unmarshal(httpResponse.entity).to[String].map{r => Serialization.read[T](r)}

  def sendTx(transaction: Transaction): Future[HttpResponse] = post("sendTx", transaction)

  def getBalance(account: PublicKey): Future[HttpResponse] = post("getBalance", account)

}