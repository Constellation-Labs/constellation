package org.constellation.util

import akka.http.scaladsl.coding.Gzip
import akka.util.ByteString
import com.softwaremill.sttp._
import com.softwaremill.sttp.json4s._
import com.softwaremill.sttp.okhttp.OkHttpFutureBackend
import com.softwaremill.sttp.prometheus.PrometheusBackend
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{CanLog, Logger}
import org.constellation.consensus.{Snapshot, SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Schema.{Id, MetricsResult}
import org.constellation.serializer.KryoSerializer
import org.constellation.{DAO}
import org.json4s.native.Serialization
import org.json4s.{Formats, native}
import org.slf4j.MDC

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object EnhancedAPIClient {

  def apply(host: String = "127.0.0.1",
            port: Int,
            peerHTTPPort: Int = 9001,
            internalPeerHost: String = "")(
    implicit executionContext: ExecutionContext,
    dao: DAO = null
  ): EnhancedAPIClient = {


    val config = ConfigFactory.load()

    val authEnabled = config.getBoolean("auth.enabled")
    val authId = config.getString("auth.id")
    val authPassword = config.getString("auth.password")

    new EnhancedAPIClient(host, port, peerHTTPPort, internalPeerHost, authEnabled, authId, authPassword)(executionContext, dao)
  }
}

class EnhancedAPIClient private(host: String = "127.0.0.1",
                                port: Int,
                                val peerHTTPPort: Int = 9001,
                                val internalPeerHost: String = "",
                                authEnabled: Boolean = false,
                                authId: String = null,
                                authPassword: String = null)(
  implicit override val executionContext: ExecutionContext,
  dao: DAO = null
) extends APIClient(host, port, authEnabled, authId, authPassword) {

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
