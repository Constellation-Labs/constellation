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

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Try}

/// New download code
object Download {

  val logger = Logger(s"Download")
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  def downloadCBFromHash(hash: String, singlePeer: PeerData)(implicit dao: DAO, ec: ExecutionContext): Unit = {
    singlePeer.client.getBlocking[Option[CheckpointBlock]]("checkpoint/" + hash) match { case Some(cb) =>
      if (cb != dao.genesisObservation.get.initialDistribution &&
        cb != dao.genesisObservation.get.initialDistribution2) {

        if (dao.dbActor.getCheckpointCacheData(cb.baseHash).isEmpty) {
          EdgeProcessor.acceptCheckpoint(cb)
          dao.metricsManager ! IncrementMetric("downloadedBlocks")
        } else {
          dao.metricsManager ! IncrementMetric("downloadedBlockButHashAlreadyExistsInDB")
        }
      }
    case None =>
      dao.metricsManager ! IncrementMetric("downloadBlockEmptyResponse")
    }
  }


  def downloadActual()(implicit dao: DAO, ec: ExecutionContext): Unit = {
    logger.info("Download started")
    dao.nodeState = NodeState.DownloadInProgress

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
    // To not download from other nodes currently downloading, fix with NodeStatus metadata
    // val (id, activeTips) = allTips.filter { case (_, t) => genesis.notGenesisTips(t) }.head

    val startingCBs = snapshotInfo.acceptedCBSinceSnapshot ++ snapshotInfo.snapshot.checkpointBlocks

    dao.dbActor.putSnapshot(snapshotInfo.snapshot.hash, snapshotInfo.snapshot)


    def getSnapshots(hash: String, blocks: Seq[String] = Seq()): Seq[String] = {
      val sn = snapshotClient.getBlocking[Option[Snapshot]]("snapshot/" + hash)
      sn match {
        case Some(snapshot) =>
          dao.dbActor.putSnapshot(hash, snapshot)
          if (snapshot.lastSnapshot == "") {
            dao.metricsManager ! IncrementMetric("downloadSnapshotEmptyStr")
            blocks
          } else {
            dao.metricsManager ! IncrementMetric("downloadedSnapshotHashes")
            getSnapshots(snapshot.lastSnapshot, blocks ++ snapshot.checkpointBlocks)
          }
        case None =>
          dao.metricsManager ! IncrementMetric("downloadSnapshotQueryFailed")
          blocks
      }
    }

    val snapshotBlocks = getSnapshots(snapshotInfo.snapshot.lastSnapshot) ++ startingCBs
    val snapshotBlocksDistinct = snapshotBlocks.distinct

    dao.metricsManager ! UpdateMetric("downloadExpectedNumBlocks", snapshotBlocks.size.toString)
    dao.metricsManager ! UpdateMetric("downloadExpectedNumBlocksDistinct", snapshotBlocksDistinct.size.toString)

    val grouped = snapshotBlocksDistinct.grouped(snapshotBlocksDistinct.size / peerData.size).toSeq.zip(peerData.values)

    val downloadRes = grouped.par.map{
      case (hashes, peer) =>
        hashes.par.map{
          hash =>
            downloadCBFromHash(hash, peer)
            Seq()
        }
    }

    downloadRes.flatten.toList

    logger.debug("First pass download finished")

    dao.metricsManager ! UpdateMetric("downloadFirstPassComplete", "true")

    dao.edgeProcessor ! DownloadComplete(snapshotInfo.snapshot)

  }

  def download()(implicit dao: DAO, ec: ExecutionContext): Unit = {

    Try {
      downloadActual()
    } match {
      case Failure(e) => e.printStackTrace()
      case _ => logger.info("Download succeeded")
    }

  }


}