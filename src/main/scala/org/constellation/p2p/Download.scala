package org.constellation.p2p

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.DAO
import org.constellation.consensus._
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Try}

/// New download code
object Download {

  val logger = Logger(s"Download")
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)


  def downloadActual()(implicit dao: DAO, ec: ExecutionContext): Unit = {
    logger.info("Download started")
    dao.nodeState = NodeState.DownloadInProgress
    PeerManager.broadcastNodeState()

    val res = (dao.peerManager ? APIBroadcast(_.getBlocking[Option[GenesisObservation]]("genesis")))
      .mapTo[Map[Id, Option[GenesisObservation]]].get()

    // TODO: Error handling and verification
    val genesis = res.filter {
      _._2.nonEmpty
    }.map {
      _._2.get
    }.head
    dao.acceptGenesis(genesis)

    dao.metricsManager ! UpdateMetric("downloadedGenesis", "true")

    val peerData = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().filter{_._2.peerMetadata.nodeState == NodeState.Ready}

    val snapshotClient = peerData.head._2.client

    val snapshotInfo = snapshotClient.getBlocking[SnapshotInfo]("info")

    val latestSnapshot = snapshotInfo.snapshot

    val preExistingSnapshots = dao.snapshotPath.list.toSeq.map{_.name}

    val snapshotHashes = snapshotClient.getBlocking[Seq[String]]("snapshotHashes").filterNot(preExistingSnapshots.contains)

    dao.metricsManager ! UpdateMetric("downloadExpectedNumSnapshots", snapshotHashes.size.toString)

    val grouped = snapshotHashes.grouped(snapshotHashes.size / peerData.size).toSeq.zip(peerData.values)

    // TODO: Move elsewhere unify with other code.
    def acceptSnapshot(r: StoredSnapshot) = {
      r.checkpointCache.foreach{dao.threadSafeTipService.accept}
      dao.dbActor.putSnapshot(r.snapshot.hash, r.snapshot)
      import better.files._
      File(dao.snapshotPath, r.snapshot.hash).writeByteArray(KryoSerializer.serializeAnyRef(r))
    }

    val downloadRes = grouped.par.map{
      case (hashes, peer) =>
        hashes.par.map{
          hash =>
            val res = peer.client.getBlocking[Option[StoredSnapshot]]("storedSnapshot/" + hash)
            res.foreach{
              r =>
                acceptSnapshot(r)
            }
            Seq()
        }
    }

    downloadRes.flatten.toList

    logger.debug("First pass download finished")

    dao.metricsManager ! UpdateMetric("downloadFirstPassComplete", "true")

    dao.generateRandomTX = true
    dao.nodeState = NodeState.Ready
    dao.metricsManager ! UpdateMetric("nodeState", dao.nodeState.toString)
    dao.peerManager ! APIBroadcast(_.post("status", SetNodeStatus(dao.id, NodeState.Ready)))
    dao.threadSafeTipService.setSnapshot(latestSnapshot)


  }

  def download()(implicit dao: DAO, ec: ExecutionContext): Unit = {

    tryWithMetric(downloadActual(), "download")

  }


}