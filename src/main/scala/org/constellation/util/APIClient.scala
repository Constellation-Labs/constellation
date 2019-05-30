package org.constellation.util

import akka.http.scaladsl.coding.Gzip
import akka.util.ByteString
import cats.effect.IO
import com.softwaremill.sttp._
import com.softwaremill.sttp.json4s.asJson
import com.typesafe.config.ConfigFactory
import org.constellation.DAO
import org.constellation.consensus.{Snapshot, SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.PeerData
import org.constellation.primitives.Schema.{Id, MetricsResult}
import org.constellation.serializer.KryoSerializer
import org.json4s.Formats
import org.json4s.native.Serialization

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object APIClient {

  def apply(host: String = "127.0.0.1",
            port: Int,
            peerHTTPPort: Int = 9001,
            internalPeerHost: String = "")(
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
class APIClient private (host: String = "127.0.0.1",
                         port: Int,
                         val peerHTTPPort: Int = 9001,
                         val internalPeerHost: String = "",
  val authEnabled: Boolean = false,
  val authId: String = null,
  authPassword: String = null)(
  implicit override val executionContext: ExecutionContext,
  dao: DAO = null
) extends APIClientBase(host, port, authEnabled, authId, authPassword) {

  var id: Id = _

  val daoOpt = Option(dao)

  override def optHeaders: Map[String, String] =
    daoOpt
      .map { d =>
        Map("Remote-Address" -> d.externalHostString, "X-Real-IP" -> d.externalHostString)
      }
      .getOrElse(Map())

  def metrics: Map[String, String] = {
    getBlocking[MetricsResult]("metrics", timeout = 5.seconds).metrics
  }

  def getBlockingBytesKryo[T <: AnyRef](suffix: String,
                                        queryParams: Map[String, String] = Map(),
                                        timeout: Duration = 5.seconds): T = {
    val resp =
      httpWithAuth(suffix, queryParams, timeout)(Method.GET).response(asByteArray).send().blocking()
    KryoSerializer.deserializeCast[T](resp.unsafeBody)
  }

  def getNonBlockingBytesKryo[T <: AnyRef](suffix: String,
                                           queryParams: Map[String, String] = Map(),
                                           timeout: Duration = 5.seconds): Future[T] = {
    httpWithAuth(suffix, queryParams, timeout)(Method.GET)
      .response(asByteArray)
      .send()
      .map(resp => KryoSerializer.deserializeCast[T](resp.unsafeBody))
  }

  def getNonBlockingIO[T <: AnyRef](
                                   suffix: String,
                                   queryParams: Map[String, String] = Map(),
                                   timeout: Duration = 5.seconds
                                 )(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): IO[T] = {
    IO.fromFuture(IO { getNonBlocking[T](suffix, queryParams, timeout) })
  }

  def postNonBlockingIO[T <: AnyRef](suffix: String,
                                     b: AnyRef,
                                     timeout: Duration = 5.seconds,
                                     headers: Map[String, String] = Map.empty)(
                                      implicit m: Manifest[T],
                                      f: Formats = constellation.constellationFormats
                                    ): IO[T] = {
    IO.fromFuture(IO { postNonBlocking[T](suffix,b,timeout,headers) })
  }

  def getSnapshotInfo(): SnapshotInfo = getBlocking[SnapshotInfo]("info")

  def getSnapshots(): Seq[Snapshot] = {

    val snapshotInfo = getSnapshotInfo()

    val startingSnapshot = snapshotInfo.snapshot

    def getSnapshots(hash: String, snapshots: Seq[Snapshot] = Seq()): Seq[Snapshot] = {
      val sn = getBlocking[Option[Snapshot]]("snapshot/" + hash)
      sn match {
        case Some(snapshot) =>
          if (snapshot.lastSnapshot == "" || snapshot.lastSnapshot == Snapshot.snapshotZeroHash) {
            snapshots :+ snapshot
          } else {
            getSnapshots(snapshot.lastSnapshot, snapshots :+ snapshot)
          }
        case None =>
          logger.warn("MISSING SNAPSHOT")
          snapshots
      }
    }

    val snapshots = getSnapshots(startingSnapshot.lastSnapshot, Seq(startingSnapshot))
    snapshots
  }

  def simpleDownload(): Seq[StoredSnapshot] = {

    val hashes = getBlocking[Seq[String]]("snapshotHashes")

    hashes.map { h =>
      getBlockingBytesKryo[StoredSnapshot]("storedSnapshot/" + h)
    }

  }

}
