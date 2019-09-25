package org.constellation.util

import cats.effect.{Async, ContextShift, IO}
import com.softwaremill.sttp._
import com.typesafe.config.ConfigFactory
import org.constellation.consensus.{SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Schema.{Id, MetricsResult}
import org.constellation.serializer.KryoSerializer
import org.constellation.{ConstellationExecutionContext, DAO}
import org.json4s.Formats

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object APIClient {

  def apply(host: String = "127.0.0.1", port: Int, peerHTTPPort: Int = 9001, internalPeerHost: String = "")(
    implicit backend: SttpBackend[Future, Nothing],
    dao: DAO = null
  ): APIClient = {

    val config = ConfigFactory.load()

    val authEnabled = config.getBoolean("auth.enabled")
    val authId = config.getString("auth.id")
    val authPassword = config.getString("auth.password")

    new APIClient(host, port, peerHTTPPort, internalPeerHost, authEnabled, authId, authPassword)(
      backend,
      dao
    )
  }
}
case class PeerApiClient(id: Id, client: APIClient)

class APIClient private (
  host: String = "127.0.0.1",
  port: Int,
  val peerHTTPPort: Int = 9001,
  val internalPeerHost: String = "",
  val authEnabled: Boolean = false,
  val authId: String = null,
  authPassword: String = null
)(
  implicit backend: SttpBackend[Future, Nothing],
  dao: DAO = null
) extends APIClientBase(host, port, authEnabled, authId, authPassword) {

  var id: Id = _

  val daoOpt = Option(dao)

  override def optHeaders: Map[String, String] =
    daoOpt.map { d =>
      Map("Remote-Address" -> d.externalHostString, "X-Real-IP" -> d.externalHostString)
    }.getOrElse(Map())

  def metricsAsync: Future[Map[String, String]] =
    getNonBlocking[MetricsResult]("metrics", timeout = 15.seconds)
      .map(_.metrics)(ConstellationExecutionContext.unbounded)

  def getBlockingBytesKryo[T <: AnyRef](
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  ): T = {
    val resp =
      httpWithAuth(suffix, queryParams, timeout)(Method.GET).response(asByteArray).send().blocking()
    KryoSerializer.deserializeCast[T](resp.unsafeBody)
  }

  def getNonBlockingArrayByteIO(
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  )(
    contextToReturn: ContextShift[IO]
  ): IO[Array[Byte]] =
    contextToReturn.evalOn(ConstellationExecutionContext.unbounded)(Async[IO].async { cb =>
      httpWithAuth(suffix, queryParams, timeout)(Method.GET)
        .response(asByteArray)
        .send()
        .onComplete {
          case Success(value: Response[Array[Byte]]) => cb(Right(value.unsafeBody))
          case Failure(error: Throwable)             => cb(Left(error))
        }(ConstellationExecutionContext.unbounded)
    })

  def getNonBlockingIO[T <: AnyRef](
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  )(
    contextToReturn: ContextShift[IO]
  )(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): IO[T] =
    contextToReturn.evalOn(ConstellationExecutionContext.unbounded)(Async[IO].async { cb =>
      getNonBlocking[T](suffix, queryParams, timeout).onComplete {
        case Success(value) => cb(Right(value))
        case Failure(error) => cb(Left(error))
      }(ConstellationExecutionContext.unbounded)
    })

  def getNonBlockingF[F[_]: Async, T <: AnyRef](
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  )(contextToReturn: ContextShift[F])(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): F[T] =
    contextToReturn.evalOn(ConstellationExecutionContext.unbounded)(Async[F].async { cb =>
      getNonBlocking[T](suffix, queryParams, timeout).onComplete {
        case Success(value) => cb(Right(value))
        case Failure(error) => cb(Left(error))
      }(ConstellationExecutionContext.unbounded)
    })

  def postNonBlockingIO[T <: AnyRef](
    suffix: String,
    b: AnyRef,
    timeout: Duration = 15.seconds,
    headers: Map[String, String] = Map.empty
  )(
    contextToReturn: ContextShift[IO]
  )(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): IO[T] =
    contextToReturn.evalOn(ConstellationExecutionContext.unbounded)(Async[IO].async { cb =>
      postNonBlocking[T](suffix, b, timeout, headers).onComplete {
        case Success(value) => cb(Right(value))
        case Failure(error) => cb(Left(error))
      }(ConstellationExecutionContext.unbounded)
    })

  def postNonBlockingF[F[_]: Async, T <: AnyRef](
    suffix: String,
    b: AnyRef,
    timeout: Duration = 15.seconds,
    headers: Map[String, String] = Map.empty
  )(contextToReturn: ContextShift[F])(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): F[T] =
    contextToReturn.evalOn(ConstellationExecutionContext.unbounded)(Async[F].async { cb =>
      postNonBlocking[T](suffix, b, timeout, headers).onComplete {
        case Success(value) => cb(Right(value))
        case Failure(error) => cb(Left(error))
      }(ConstellationExecutionContext.unbounded)
    })

  def postNonBlockingUnitF[F[_]: Async](
    suffix: String,
    b: AnyRef,
    timeout: Duration = 15.seconds,
    headers: Map[String, String] = Map.empty
  )(contextToReturn: ContextShift[F])(implicit f: Formats = constellation.constellationFormats): F[Response[Unit]] =
    contextToReturn.evalOn(ConstellationExecutionContext.unbounded)(Async[F].async { cb =>
      postNonBlockingUnit(suffix, b, timeout).onComplete {
        case Success(value) => cb(Right(value))
        case Failure(error) => cb(Left(error))
      }(ConstellationExecutionContext.unbounded)
    })

  def getStringF[F[_]: Async](
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  )(contextToReturn: ContextShift[F]): F[Response[String]] =
    contextToReturn.evalOn(ConstellationExecutionContext.unbounded)(Async[F].async { cb =>
      httpWithAuth(suffix, queryParams, timeout)(Method.GET)
        .send()
        .onComplete {
          case Success(value) => cb(Right(value))
          case Failure(error) => cb(Left(error))
        }(ConstellationExecutionContext.unbounded)
    })

  def postNonBlockingIOUnit(
    suffix: String,
    b: AnyRef,
    timeout: Duration = 15.seconds,
    headers: Map[String, String] = Map.empty
  )(contextToReturn: ContextShift[IO])(implicit f: Formats = constellation.constellationFormats): IO[Response[Unit]] =
    contextToReturn.evalOn(ConstellationExecutionContext.unbounded)(Async[IO].async { cb =>
      postNonBlockingUnit(suffix, b, timeout, headers).onComplete {
        case Success(value) => cb(Right(value))
        case Failure(error) => cb(Left(error))
      }(ConstellationExecutionContext.unbounded)
    })

  def simpleDownload(): Seq[StoredSnapshot] = {
    val hashes = getBlocking[Seq[String]]("snapshotHashes")
    hashes.map(hash => getBlockingBytesKryo[StoredSnapshot]("storedSnapshot/" + hash))
  }

  def snapshotsInfoDownload(): SnapshotInfo =
    getBlockingBytesKryo[SnapshotInfo]("info")

}
