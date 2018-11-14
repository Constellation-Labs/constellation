package org.constellation.p2p

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.DAO
import org.constellation.consensus.SnapshotTrigger.acceptCheckpoint
import org.constellation.consensus._
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.APIClient

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Try}

/// New download code
object Download {

  val logger = Logger(s"Download")
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  // Add warning for empty peers
  def downloadActual()(implicit dao: DAO, ec: ExecutionContext): Unit = {
    logger.info("Download started")
    dao.nodeState = NodeState.DownloadInProgress
    PeerManager.broadcastNodeState()

    dao.peerInfo.map{_._2.client}.foreach{
      _.post("faucet", SendToAddress(dao.selfAddressStr, 500L))
    }

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

    logger.info(s"Downloading from: ${snapshotClient.hostName}:${snapshotClient.apiPort}")

    val snapshotInfo = snapshotClient.getBlockingBytesKryo[SnapshotInfo]("info", timeoutSeconds = 300)

    dao.metricsManager ! UpdateMetric("downloadExpectedNumSnapshotsIncludingPreExisting", snapshotInfo.snapshotHashes.size.toString)

    val preExistingSnapshots = dao.snapshotPath.list.toSeq.map{_.name}

    val snapshotHashes = snapshotInfo.snapshotHashes.filterNot(preExistingSnapshots.contains)

    dao.metricsManager ! UpdateMetric("downloadExpectedNumSnapshots", snapshotHashes.size.toString)

    val groupSize = (snapshotHashes.size / peerData.size) + 1

    dao.metricsManager ! UpdateMetric("downloadGroupSize", groupSize.toString)

    val groupedHashes = snapshotHashes.grouped(groupSize).toSeq

    dao.metricsManager ! UpdateMetric("downloadGroupedHashesSize", groupedHashes.size.toString)

    val grouped = groupedHashes.zip(peerData.values)
    dao.metricsManager ! UpdateMetric("downloadGroupedZipSize", grouped.size.toString)

    dao.metricsManager ! UpdateMetric("downloadGroupCheckSize", grouped.flatMap(_._1).size.toString)

    // TODO: Move elsewhere unify with other code.
    def acceptSnapshot(r: StoredSnapshot) = {
      r.checkpointCache.foreach{
        c =>
          dao.metricsManager ! IncrementMetric("downloadedBlocks")
          // Bypasses tip update / accumulating acceptedSinceCB

          // TODO: Rebuild ledger and verify, turning off for debug
          dao.metricsManager ! IncrementMetric("checkpointAccepted")

          //tryWithMetric(acceptCheckpoint(c), "acceptCheckpoint")
          c.checkpointBlock.foreach{
            _.transactions.foreach{
              tx =>
                //     tx.ledgerApplySnapshot()
                dao.metricsManager ! IncrementMetric("transactionAccepted")
              // dao.transactionService.delete(Set(tx.hash))
            }
          }

      }
      //dao.dbActor.putSnapshot(r.snapshot.hash, r.snapshot)
      import better.files._
      File(dao.snapshotPath, r.snapshot.hash).writeByteArray(KryoSerializer.serializeAnyRef(r))
    }

    def processSnapshotHash(peer: APIClient, hash: String): Boolean = {

      var activePeer = peer
      var remainingPeers : Seq[APIClient] = peerData.values.map{_.client}.filterNot(_ == activePeer).toSeq

      var done = false

      while (!done && remainingPeers.nonEmpty) {

        val res = Try{activePeer.getBlockingBytesKryo[StoredSnapshot]("storedSnapshot/" + hash, timeoutSeconds = 100)}
        res match {
          case Failure(e) => e.printStackTrace()
          case _ =>
        }
        res.toOption.foreach{
          r =>
            dao.metricsManager ! IncrementMetric("downloadedSnapshots")
            dao.metricsManager ! IncrementMetric("snapshotCount")
            acceptSnapshot(r)
        }
        if (res.isFailure) {
          dao.metricsManager ! IncrementMetric("downloadSnapshotDataFailed")
        }
        done = res.isSuccess

        if (!done && remainingPeers.nonEmpty) {
          activePeer = remainingPeers.head
          remainingPeers = remainingPeers.filterNot(_ == activePeer)
        }

      }

      done
    }

    val downloadRes = grouped.par.map{
      case (hashes, peer) =>
        hashes.par.map{ hash =>
          processSnapshotHash(peer.client, hash)
        }
    }

    downloadRes.flatten.toList
    dao.metricsManager ! UpdateMetric("downloadFirstPassComplete", "true")
    dao.nodeState = NodeState.DownloadCompleteAwaitingFinalSync
    dao.metricsManager ! UpdateMetric("nodeState", dao.nodeState.toString)

    // Thread.sleep(10*1000)

    val snapshotInfo2 = snapshotClient.getBlockingBytesKryo[SnapshotInfo]("info", timeoutSeconds = 300)

    val snapshotHashes2 = snapshotInfo2.snapshotHashes
      .filterNot(preExistingSnapshots.contains)
      .filterNot(snapshotHashes.contains)

    dao.metricsManager ! UpdateMetric("downloadExpectedNumSnapshotsSecondPass", snapshotHashes2.size.toString)


    val groupSize2Original = snapshotHashes2.size / peerData.size
    val groupSize2 = Math.max(groupSize2Original, 1)
    val grouped2 = snapshotHashes2.grouped(groupSize2).toSeq.zip(peerData.values)

    val downloadRes2 = grouped2.par.map{
      case (hashes, peer) =>
        hashes.par.map{ hash =>
          processSnapshotHash(peer.client, hash)
        }
    }

    downloadRes2.flatten.toList
    dao.metricsManager ! UpdateMetric("downloadSecondPassComplete", "true")

    logger.debug("First pass download finished")

    dao.threadSafeTipService.setSnapshot(snapshotInfo2)
    dao.generateRandomTX = true
    dao.nodeState = NodeState.Ready

    dao.threadSafeTipService.syncBuffer.foreach{ h =>

      if (!snapshotInfo2.acceptedCBSinceSnapshotCache.contains(h) && !snapshotInfo2.snapshotCache.contains(h)) {
        dao.metricsManager ! IncrementMetric("syncBufferCBAccepted")
        dao.threadSafeTipService.accept(h)
        /*        dao.metricsManager ! IncrementMetric("checkpointAccepted")
                dao.checkpointService.put(h.checkpointBlock.get.baseHash, h)
                h.checkpointBlock.get.transactions.foreach {
                  _ =>
                    dao.metricsManager ! IncrementMetric("transactionAccepted")
                }*/
      }
    }

    dao.threadSafeTipService.syncBuffer = Seq()

    dao.metricsManager ! UpdateMetric("nodeState", dao.nodeState.toString)
    dao.peerManager ! APIBroadcast(_.post("status", SetNodeStatus(dao.id, NodeState.Ready)))
    dao.downloadFinishedTime = System.currentTimeMillis()
    dao.transactionAcceptedAfterDownload = (dao.metricsManager ? GetMetrics)
      .mapTo[Map[String, String]].get().get("transactionAccepted").map{_.toLong}.getOrElse(0L)


  }

  def download()(implicit dao: DAO, ec: ExecutionContext): Unit = {


    tryWithMetric(downloadActual(), "download")
    /*val maxRetries = 5
    var attemptNum = 0
    var done = false

    while (attemptNum <= maxRetries && !done) {

      done = tryWithMetric(downloadActual(), "download").isSuccess
      attemptNum += 1
      Thread.sleep(5000)

    }
*/


  }


}