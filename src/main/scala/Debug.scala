

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers, Unmarshal}
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.constellation.blockchain.Transaction
import org.constellation.wallet.Wallet
import org.json4s.{DefaultFormats, Formats, native}
import org.json4s.native.Serialization

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Random


// TODO : Implement all methods from RPCInterface here for a client SDK

class RPCClient(host: String = "127.0.0.1", port: Int)(
  implicit val system: ActorSystem,
  implicit val materialize: ActorMaterializer,
  implicit val executionContext: ExecutionContextExecutor,
) {

  val baseURI = s"http://$host:$port"
  def base(suffix: String) = Uri(s"$baseURI/$suffix")
  def query(suffix: String, queryParams: Map[String,String] = Map()): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(uri = base(suffix).withQuery(Query(queryParams)))
    )
  }

  def post[T <: AnyRef](suffix: String, t: T)(implicit f : Formats): Future[HttpResponse] = {
    val ser = Serialization.write(t)
    Http().singleRequest(
      HttpRequest(uri = base(suffix), method = HttpMethods.POST, entity = HttpEntity(
        ContentTypes.`application/json`, ser)
      )
    )
  }

  def createWallet(numKeyPairs: Int = 1): Future[HttpResponse] =
    query("createWallet", Map("numPairs" -> numKeyPairs.toString))

  def wallet(): Future[HttpResponse] = query("wallet")

}

object Debug {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.empty().withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(2556))
    implicit val system: ActorSystem = ActorSystem("BlockChain", conf)
    implicit val materialize: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    implicit class EasyFutureBlock[T](f: Future[T]) {
      def get(t: Int = 5): T = {
        import scala.concurrent.duration._
        Await.result(f, t.seconds)
      }
    }

    val seedRPC = new RPCClient(port=8080)

    implicit val serialization: Serialization.type = native.Serialization
    implicit val stringUnmarshallers: FromEntityUnmarshaller[String] = PredefinedFromEntityUnmarshallers.stringUnmarshaller

    implicit def json4sFormats: Formats = DefaultFormats

    def uread[T <: AnyRef](httpResponse: HttpResponse)(implicit m : Manifest[T]) =
      Serialization.read[T](Unmarshal(httpResponse.entity).to[String].get())


    val response = seedRPC.post("sendTx", Transaction("hashpointer", "id", 1L, "key", "key2", 5L, "sig"))

    println(response.get())
  //  println(uread[String](response.get()))




  }
}