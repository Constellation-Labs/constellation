package org.constellation.p2p

import akka.pattern.ask
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.consensus._
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.APIClient

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

/// New download code

object Download extends StrictLogging {

  // Add warning for empty peers

  def downloadActual()(implicit dao: DAO, ec: ExecutionContext): Unit = {
    logger.info("Download started")
    dao.nodeState = NodeState.DownloadInProgress
    PeerManager.broadcastNodeState()

    dao.peerInfo.map { _._2.client }.foreach {
      _.post("faucet", SendToAddress(dao.selfAddressStr, 500L))
    }

    // TODO: Error handling and verification
    val res =
      (dao.peerManager ? APIBroadcast(_.getNonBlocking[Option[GenesisObservation]]("genesis")))
        .mapTo[Map[Id, Future[Option[GenesisObservation]]]]

    val genF = res.flatMap { m =>
      Future.find(m.values.toList)(_.nonEmpty).map(_.flatten.get)
    }

    val genesis = genF.get()
    dao.acceptGenesis(genesis)

    dao.metrics.updateMetric("downloadedGenesis", "true")

    val peerData = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().filter {
      _._2.peerMetadata.nodeState == NodeState.Ready
    }

    val snapshotClient = peerData.head._2.client

    logger.info(s"Downloading from: ${snapshotClient.hostName}:${snapshotClient.apiPort}")

    val snapshotInfo =
      snapshotClient.getBlockingBytesKryo[SnapshotInfo]("info", timeout = 300.seconds)

    dao.metrics.updateMetric("downloadExpectedNumSnapshotsIncludingPreExisting",
                             snapshotInfo.snapshotHashes.size.toString)

    val preExistingSnapshots = dao.snapshotPath.list.toSeq.map { _.name }

    val snapshotHashes = snapshotInfo.snapshotHashes.filterNot(preExistingSnapshots.contains)

    dao.metrics.updateMetric("downloadExpectedNumSnapshots", snapshotHashes.size.toString)

    val groupSize = (snapshotHashes.size / peerData.size) + 1

    dao.metrics.updateMetric("downloadGroupSize", groupSize.toString)

    val groupedHashes = snapshotHashes.grouped(groupSize).toSeq

    dao.metrics.updateMetric("downloadGroupedHashesSize", groupedHashes.size.toString)

    val grouped = groupedHashes.zip(peerData.values)
    dao.metrics.updateMetric("downloadGroupedZipSize", grouped.size.toString)

    dao.metrics.updateMetric("downloadGroupCheckSize", grouped.flatMap(_._1).size.toString)

    // TODO: Move elsewhere unify with other code.

    def acceptSnapshot(r: StoredSnapshot) = {
      r.checkpointCache.foreach { c =>
        dao.metrics.incrementMetric("downloadedBlocks")
        // Bypasses tip update / accumulating acceptedSinceCB

        // TODO: Rebuild ledger and verify, turning off for debug
        dao.metrics.incrementMetric("checkpointAccepted")

        //tryWithMetric(acceptCheckpoint(c), "acceptCheckpoint")
        c.checkpointBlock.foreach {
          _.transactions.foreach { tx =>
            //     tx.ledgerApplySnapshot()
            dao.metrics.incrementMetric("transactionAccepted")
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
      var remainingPeers: Seq[APIClient] =
        peerData.values.map { _.client }.filterNot(_ == activePeer).toSeq

      var done = false

      while (!done && remainingPeers.nonEmpty) {

        val res = Try {
          activePeer
            .getBlockingBytesKryo[StoredSnapshot]("storedSnapshot/" + hash, timeout = 100.seconds)
        }
        res match {
          case Failure(e) => e.printStackTrace()
          case _          =>
        }
        res.toOption.foreach { r =>
          dao.metrics.incrementMetric("downloadedSnapshots")
          dao.metrics.incrementMetric("snapshotCount")
          acceptSnapshot(r)
        }
        if (res.isFailure) {
          dao.metrics.incrementMetric("downloadSnapshotDataFailed")
        }
        done = res.isSuccess

        if (!done && remainingPeers.nonEmpty) {
          activePeer = remainingPeers.head
          remainingPeers = remainingPeers.filterNot(_ == activePeer)
        }

      }

      done
    }

    val downloadRes = grouped.par.map {
      case (hashes, peer) =>
        hashes.par.map { hash =>
          processSnapshotHash(peer.client, hash)
        }
    }

    downloadRes.flatten.toList
    dao.metrics.updateMetric("downloadFirstPassComplete", "true")
    dao.nodeState = NodeState.DownloadCompleteAwaitingFinalSync
    dao.metrics.updateMetric("nodeState", dao.nodeState.toString)

    // Thread.sleep(10*1000)

    val snapshotInfo2 =
      snapshotClient.getBlockingBytesKryo[SnapshotInfo]("info", timeout = 300.seconds)

    val snapshotHashes2 = snapshotInfo2.snapshotHashes
      .filterNot(preExistingSnapshots.contains)
      .filterNot(snapshotHashes.contains)

    dao.metrics
      .updateMetric("downloadExpectedNumSnapshotsSecondPass", snapshotHashes2.size.toString)

    val groupSize2Original = snapshotHashes2.size / peerData.size
    val groupSize2 = Math.max(groupSize2Original, 1)
    val grouped2 = snapshotHashes2.grouped(groupSize2).toSeq.zip(peerData.values)

    val downloadRes2 = grouped2.par.map {
      case (hashes, peer) =>
        hashes.par.map { hash =>
          processSnapshotHash(peer.client, hash)
        }
    }

    downloadRes2.flatten.toList
    dao.metrics.updateMetric("downloadSecondPassComplete", "true")

    logger.debug("First pass download finished")

    dao.threadSafeTipService.setSnapshot(snapshotInfo2)
    dao.generateRandomTX = true
    dao.setNodeState(NodeState.Ready)

    dao.threadSafeTipService.syncBuffer.foreach { h =>
      if (!snapshotInfo2.acceptedCBSinceSnapshotCache.contains(h) && !snapshotInfo2.snapshotCache
            .contains(h)) {
        dao.metrics.incrementMetric("syncBufferCBAccepted")
        dao.threadSafeTipService.accept(h)
        /*        dao.metrics.incrementMetric("checkpointAccepted")
                dao.checkpointService.put(h.checkpointBlock.get.baseHash, h)
                h.checkpointBlock.get.transactions.foreach {
                  _ =>
                    dao.metrics.incrementMetric("transactionAccepted")
                }*/
      }
    }

    dao.threadSafeTipService.syncBuffer = Seq()

    dao.peerManager ! APIBroadcast(_.post("status", SetNodeStatus(dao.id, NodeState.Ready)))
    dao.downloadFinishedTime = System.currentTimeMillis()
    dao.transactionAcceptedAfterDownload =
      dao.metrics.getMetrics.get("transactionAccepted").map { _.toLong }.getOrElse(0L)

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
