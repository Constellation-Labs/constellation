package org.constellation.util

import cats.effect.{Async, ContextShift, IO, LiftIO}
import cats.implicits._
import com.softwaremill.sttp._
import com.typesafe.config.ConfigFactory
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.consensus.{SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Schema.{GenesisObservation, Id, MetricsResult}
import org.constellation.serializer.KryoSerializer
import org.json4s.Formats

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object APIClient {

  def apply(host: String = "127.0.0.1", port: Int, peerHTTPPort: Int = 9001, internalPeerHost: String = "")(
    implicit executionContext: ExecutionContext,
    dao: DAO = null
  ): APIClient = {

    val config = ConfigFactory.load()

    val authEnabled = config.getBoolean("auth.enabled")
    val authId = config.getString("auth.id")
    val authPassword = config.getString("auth.password")

    new APIClient(host, port, peerHTTPPort, internalPeerHost, authEnabled, authId, authPassword)(
      executionContext,
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
  implicit override val executionContext: ExecutionContext,
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
      .map(_.metrics)(ConstellationExecutionContext.callbacks)

  def getBlockingBytesKryo[T <: AnyRef](
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  ): T = {
    val resp =
      httpWithAuth(suffix, queryParams, timeout)(Method.GET).response(asByteArray).send().blocking()
    KryoSerializer.deserializeCast[T](resp.unsafeBody)
  }

  def getNonBlockingBytesKryo[T <: AnyRef](
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  ): Future[T] =
    httpWithAuth(suffix, queryParams, timeout)(Method.GET)
      .response(asByteArray)
      .send()
      .map(resp => KryoSerializer.deserializeCast[T](resp.unsafeBody))(ConstellationExecutionContext.callbacks)

  def getNonBlockingBytesKryoTry[T <: AnyRef](
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  ): Future[Try[T]] =
    httpWithAuth(suffix, queryParams, timeout)(Method.GET)
      .response(asByteArray)
      .send()
      .map(
        resp =>
          if (resp.isSuccess) Success(KryoSerializer.deserializeCast[T](resp.unsafeBody))
          else Failure(new Exception(s"Invalid response code ${resp.code} and status ${resp.statusText}"))
      )(ConstellationExecutionContext.callbacks)

  def getNonBlockingIO[T <: AnyRef](
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  )(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): IO[T] =
    Async[IO].async { cb =>
      getNonBlocking[T](suffix, queryParams, timeout).onComplete {
        case Success(value) => cb(Right(value))
        case Failure(error) => cb(Left(error))
      }(ConstellationExecutionContext.callbacks)
    }

  def getNonBlockingF[F[_]: Async, T <: AnyRef](
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  )(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): F[T] =
    LiftIO[F].liftIO(getNonBlockingIO(suffix, queryParams, timeout))

  def postNonBlockingIO[T <: AnyRef](
    suffix: String,
    b: AnyRef,
    timeout: Duration = 15.seconds,
    headers: Map[String, String] = Map.empty
  )(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): IO[T] =
    Async[IO].async { cb =>
      postNonBlocking[T](suffix, b, timeout, headers).onComplete {
        case Success(value) => cb(Right(value))
        case Failure(error) => cb(Left(error))
      }(ConstellationExecutionContext.callbacks)
    }

  def postNonBlockingF[F[_]: LiftIO, T <: AnyRef](
    suffix: String,
    b: AnyRef,
    timeout: Duration = 15.seconds,
    headers: Map[String, String] = Map.empty
  )(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): F[T] =
    LiftIO[F].liftIO(postNonBlockingIO(suffix, b, timeout, headers))

  def postNonBlockingUnitF[F[_]: Async](
    suffix: String,
    b: AnyRef,
    timeout: Duration = 15.seconds,
    headers: Map[String, String] = Map.empty
  )(implicit f: Formats = constellation.constellationFormats): F[Response[Unit]] =
    Async[F].async { cb =>
      postNonBlockingUnit(suffix, b, timeout).onComplete {
        case Success(value) => cb(Right(value))
        case Failure(error) => cb(Left(error))
      }(ConstellationExecutionContext.callbacks)
    }

  def getStringF[F[_]: LiftIO](
    suffix: String,
    queryParams: Map[String, String] = Map(),
    timeout: Duration = 15.seconds
  )(): F[Response[String]] =
    LiftIO[F].liftIO(getStringIO(suffix, queryParams))

  def postNonBlockingIOUnit(
    suffix: String,
    b: AnyRef,
    timeout: Duration = 15.seconds,
    headers: Map[String, String] = Map.empty
  )(implicit f: Formats = constellation.constellationFormats): IO[Response[Unit]] =
    Async[IO].async { cb =>
      postNonBlockingUnit(suffix, b, timeout, headers).onComplete {
        case Success(value) => cb(Right(value))
        case Failure(error) => cb(Left(error))
      }(ConstellationExecutionContext.callbacks)
    }

  def simpleDownload(): Seq[StoredSnapshot] = {
    val hashes = getBlocking[Seq[String]]("snapshotHashes")
    hashes.map(hash => getBlockingBytesKryo[StoredSnapshot]("storedSnapshot/" + hash))
  }

  def snapshotsInfoDownload(): SnapshotInfo =
    getBlockingBytesKryo[SnapshotInfo]("info")

  def genesisDownload(): IO[Option[GenesisObservation]] =
    getNonBlockingIO[Option[GenesisObservation]]("genesis")
}
