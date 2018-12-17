package org.constellation.util

import cats.effect.{IO, Resource, Sync}
import com.typesafe.config.ConfigFactory
import fs2.Chunk
import io.circe._
import org.constellation.primitives.Schema.Id
import org.http4s.Uri._
import org.http4s.UriTemplate.{ParamElm, PathElm}
import org.http4s._
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.headers.{Authorization, `Content-Type`}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.runtime.universe._


class Http4sClient(val host: String = "127.0.0.1", val port: Option[Int] = None)(implicit ec: ExecutionContext) {

  case class Err[F[_] : Sync](request: Request[F], response: Response[F]) {
    override def toString(): String = s"HTTP ERROR : Request ${request.toString} \n Response ${response.toString}"
  }

  var udpPort: Int = 16180
  var id: Id = _
  var peerHttpPort: Int = _

  private val config = ConfigFactory.load()
  private val strDecoder = EntityDecoder.text[IO]
  private val strEncoder = EntityEncoder.simple[IO, String]()(x => Chunk.bytes(x.getBytes))

  private val authId = config.getString("auth.id")
  private val authPassword = config.getString("auth.password")
  private val authEnabled: Option[Boolean] = None

  private lazy val clientIO = Http4sClientFactory.createHttp4sClient

  private def getHeaders(headers: Map[String, String] = Map.empty): List[Header] = {
    val appHeaders = headers
      .map { case (k, v) => Header(k, v) }.toList

    val authHeader = authEnabled
      .collect { case true => Authorization(BasicCredentials(authId, authPassword)) }.toList

    appHeaders ++ authHeader
  }

  def get[T: Decoder : TypeTag](suffix: String = "",
                                params: Map[String, String] = Map.empty,
                                headers: Map[String, String] = Map.empty,
                                timeout: FiniteDuration = 5.seconds
                               ): IO[T] = {
    clientIO.use { client =>
      (for {
        uri <- getUri(host, port, suffix, params)
        request <- buildHttpRequest(headers, uri, Method.GET)
        response <- typeOf[T] match {
          case t if t =:= typeOf[String] => client.expect[String](request)(strDecoder).map(_.asInstanceOf[T])
          case _ => client.expectOr[T](request)(r =>
            IO(new Exception(Err(request, r).toString)))(jsonOf[IO, T])
        }
      } yield response)
    }
  }

  def post[T: Decoder : TypeTag](suffix: String = "",
                                 params: Map[String, String] = Map.empty,
                                 headers: Map[String, String] = Map.empty,
                                 body: String = "",
                                 timeout: FiniteDuration = 5.seconds
             ): IO[T] = {
    clientIO.use { client =>
      for {
        uri <- getUri(host, port, suffix, params)
        request <- buildHttpRequest(headers, uri, Method.POST, Some(body))
        response <- typeOf[T] match {
          case t if t =:= typeOf[String] => client.expect[String](request)(strDecoder).map(_.asInstanceOf[T])
          case _ => client.expectOr[T](request)(r => IO(new Exception(Err(request, r).toString)))(jsonOf[IO, T])
        }
      } yield response
    }
  }

  def postBlocking[T: Decoder : TypeTag](suffix: String = "",
                                         params: Map[String, String] = Map.empty,
                                         headers: Map[String, String] = Map.empty,
                                         body: String = "",
                                         timeout: FiniteDuration = 10.seconds): T =
    post[T](suffix, params, headers, body, timeout).unsafeRunSync()

  def getBlocking[T: Decoder : TypeTag](suffix: String = "",
                                        params: Map[String, String] = Map.empty,
                                        headers: Map[String, String] = Map.empty,
                                        timeout: FiniteDuration = 5.seconds
                             ): T =
    get[T](suffix, params, headers, timeout).unsafeRunSync()

  private def buildHttpRequest(headers: Map[String, String],
                               uri: Uri, method: Method, body: Option[String] = None): IO[Request[IO]] = {
    Request[IO]()
      .withMethod(method)
      .withUri(uri)
      .withHeaders(Headers(getHeaders(headers) ++ List(`Content-Type`(MediaType.application.json))))
      .withOptionalBody(body)
  }

  private def getUri(host: String, port: Option[Int], suffix: String, params: Map[String, String]): IO[Uri] = {
    val uriEither = UriTemplate(
      authority = Some(Uri.Authority(host = RegName(host), port = port)),
      path = List(PathElm(suffix)),
      query = params.map { case (k, v) => ParamElm(k, List(v)) }.toList
    ).toUriIfPossible.toEither

    IO.fromEither(uriEither)
  }

  private implicit class AddRequestBodyImplicit(req: Request[IO]) {
    def withOptionalBody(body: Option[String]): IO[Request[IO]] = body match {
      case Some(b) => IO(req.withEntity(b)(strEncoder))
      case _ => IO(req)
    }
  }
}

/*
* Singleton Http4sClient
* */
object Http4sClientFactory {
  var clientIO: Option[Resource[IO, Client[IO]]] = None

  def createHttp4sClient(implicit ec: ExecutionContext): Resource[IO, Client[IO]] = clientIO match {
    case None =>
      implicit val cs = IO.contextShift(ec)
      clientIO = Some(BlazeClientBuilder[IO](ec).resource)
      clientIO.get
    case Some(client) => client
  }
}