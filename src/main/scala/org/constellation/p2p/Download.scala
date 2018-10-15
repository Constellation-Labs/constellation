package org.constellation.p2p

import java.net.InetSocketAddress
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import com.typesafe.scalalogging.Logger
import org.constellation.DAO
import org.constellation.primitives.Schema._
import constellation._
import org.constellation.primitives._
import org.constellation.util.{APIClient, Signed}
import scalaj.http.HttpResponse

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import akka.pattern.ask
import akka.util.Timeout
import org.constellation.LevelDB.{DBGet, DBPut}
import org.constellation.consensus._

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Try}

/// New download code
object Download {

  val logger = Logger(s"Download")
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)




  def downloadHash(hash: String, singlePeer: PeerData)(implicit dao: DAO, ec: ExecutionContext): Future[Unit] = {
    val cache = (dao.dbActor ? DBGet(hash)).mapTo[Option[CheckpointCacheData]].get()
    if (cache.isEmpty) {
      singlePeer.client.get("checkpoint/" + hash).map {
        _.body.x[Option[CheckpointBlock]]
      }.map {
        _.map { cb =>
          val cbCache = (dao.dbActor ? DBGet(cb.baseHash)).mapTo[Option[CheckpointCacheData]].get()
          if (cbCache.isEmpty && cb != dao.genesisObservation.get.initialDistribution &&
            cb != dao.genesisObservation.get.initialDistribution2) {
            EdgeProcessor.acceptCheckpoint(cb)
            dao.metricsManager ! IncrementMetric("downloadedBlocks")
            Future.sequence(cb.parentSOEBaseHashes.map(h => downloadHash(h, singlePeer)))
          } else Future.unit
        }.getOrElse(Future.unit)
      }
    } else Future.unit
  }


  /*  @tailrec
  def downloadHashes(hashes: Seq[String], singlePeer: PeerData)(implicit dao: DAO, ec: ExecutionContext): Unit = {

    val res = Future.sequence(hashes.map { hash =>
      Future {
        val cache = (dao.dbActor ? DBGet(hash)).mapTo[Option[CheckpointCacheData]].get()
        if (cache.isEmpty) {
          val cbo = singlePeer.client.getSync("checkpoint/" + hash).body.x[Option[CheckpointBlock]]
          if (cbo.nonEmpty) {
            val cb = cbo.get
            val cbCache = (dao.dbActor ? DBGet(cb.baseHash)).mapTo[Option[CheckpointCacheData]].get()

            if (cbCache.isEmpty && cb != dao.genesisObservation.get.initialDistribution &&
              cb != dao.genesisObservation.get.initialDistribution2) {

              EdgeProcessor.acceptCheckpoint(cb)
              dao.metricsManager ! IncrementMetric("downloadedBlocks")
              Some(cb.parentSOEBaseHashes)
            } else None
          } else None
        } else None
      }
    }).map{_.flatten.flatten}.foreach(hs => downloadHashes(hs, singlePeer))



  }*/

  def downloadSingle(activeTips: Seq[CheckpointBlock], singlePeer: PeerData, id: Id)(implicit dao: DAO, ec: ExecutionContext): Unit = {

    activeTips.foreach{ z =>
      EdgeProcessor.acceptCheckpoint(z)
      z.parentSOEBaseHashes.foreach{ h =>
        Future(downloadHash(h, singlePeer))

      }
      //downloadHashes(z.parentSOEBaseHashes, singlePeer)
    }

    //  Thread.sleep(20*1000)

    // dao.metricsManager ! UpdateMetric("downloadedBlocks", count.toString)
    //dao.metricsManager ! UpdateMetric("downloadedBlocksMemSize", cbs.size.toString)

    logger.debug("First pass download finished")

    dao.metricsManager ! UpdateMetric("downloadFirstPassComplete", "true")
    /*

        cbs.map { case (_, cb) =>
          // Blocks may have been accepted in the mean time before this gets called
          if ((dao.dbActor ? DBGet(cb.baseHash)).mapTo[Option[CheckpointCacheData]].get().isEmpty) {
            EdgeProcessor.acceptCheckpoint(cb)
          }
        }
    */

    dao.nodeState = NodeState.Ready
    dao.peerManager ! APIBroadcast(_.post("status", SetNodeStatus(dao.id, NodeState.Ready)))

  }



  def downloadJustOneHash(hash: String, singlePeer: PeerData)(implicit dao: DAO, ec: ExecutionContext): Unit = {
    singlePeer.client.getBlocking[Option[CheckpointBlock]]("checkpoint/" + hash) match { case Some(cb) =>
      if (cb != dao.genesisObservation.get.initialDistribution &&
        cb != dao.genesisObservation.get.initialDistribution2) {

        if ((dao.dbActor ? DBGet(cb.baseHash)).mapTo[Option[CheckpointBlock]].get().isEmpty) {
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


  def download()(implicit dao: DAO, ec: ExecutionContext): Unit = {

    Try {
      logger.info("Download started")
      dao.nodeState = NodeState.DownloadInProgress

      val res = (dao.peerManager ? APIBroadcast(_.getSync("genesis").body.x[Option[GenesisObservation]]))
        .mapTo[Map[Id, Option[GenesisObservation]]].get()

      // TODO: Error handling and verification
      val genesis = res.filter {
        _._2.nonEmpty
      }.map {
        _._2.get
      }.head
      dao.acceptGenesis(genesis)

      dao.metricsManager ! UpdateMetric("downloadedGenesis", "true")

      /*
            val allTips = (dao.peerManager ? APIBroadcast(_.getSync("tips").body.x[Seq[CheckpointBlock]]))
              .mapTo[Map[Id, Seq[CheckpointBlock]]].get()
      */

      val peerData = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().filter{_._2.nodeState == NodeState.Ready}

      val snapshotPeer = peerData.head._1
      val snapshotClient = peerData.head._2.client

      val snapshotInfo = snapshotClient.getBlocking[SnapshotInfo]("info")
      // To not download from other nodes currently downloading, fix with NodeStatus metadata
      // val (id, activeTips) = allTips.filter { case (_, t) => genesis.notGenesisTips(t) }.head

      val startingCBs = snapshotInfo.acceptedCBSinceSnapshot ++ snapshotInfo.snapshot.checkpointBlocks

      dao.dbActor ! DBPut(snapshotInfo.snapshot.hash, snapshotInfo.snapshot)


      def getSnapshots(hash: String, blocks: Seq[String] = Seq()): Seq[String] = {
        val sn = snapshotClient.getBlocking[Option[Snapshot]]("snapshot/" + hash)
        sn match {
          case Some(snapshot) =>
            dao.dbActor ! DBPut(hash, snapshot)
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
              downloadJustOneHash(hash, peer)
              Seq()
          }
      }

      downloadRes.flatten.toList

  //    dao.metricsManager ! UpdateMetric("downloadedActiveTips", activeTips.size.toString)

    //  val singlePeer = peerData(id)

     // dao.metricsManager ! UpdateMetric("downloadedActiveTips", activeTips.size.toString)

     // downloadSingle(activeTips, singlePeer, id)

      //val peerIds = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq

    //  dao.nodeState = NodeState.Ready

      logger.debug("First pass download finished")

      dao.metricsManager ! UpdateMetric("downloadFirstPassComplete", "true")

      dao.edgeProcessor ! DownloadComplete(snapshotInfo.snapshot)

    } match {
      case Failure(e) => e.printStackTrace()
      case _ => logger.info("Download succeeded")
    }

  }


}

/*


#4



Solution #3

   // val cbs = TrieMap[String, CheckpointBlock]()

   // @volatile var count = 0

    val parentsAwaitingDownload = new ConcurrentLinkedQueue[String]()
    val threadsFinished = TrieMap[Int, Boolean]()

    activeTips.foreach{ z =>
      EdgeProcessor.acceptCheckpoint(z)
      z.parentSOEBaseHashes.foreach{h =>
        parentsAwaitingDownload.add(h)
      }
    }

    val par = Seq.fill(100)(0)
    val downloadResult = par.map{ i =>

      Future {
        threadsFinished(i) = false

        do {

          val hash = parentsAwaitingDownload.poll()
          if (hash != null) {
            threadsFinished(i) = false
            val cache = (dao.dbActor ? DBGet(hash)).mapTo[Option[CheckpointCacheData]].get()
            if (cache.isEmpty) {

/* // TODO: Investigate bug here with ECs

              val askRes = dao.peerManager ? APIBroadcast(_.get("checkpoint/" + hash).map {
                _.body.x[Option[CheckpointBlock]]
              }, peerSubset = Set(id))

              val result = askRes.mapTo[Map[Id, Future[Option[CheckpointBlock]]]].flatMap {_.head._2}
              val cbo = result.get()
*/

              val cbo = singlePeer.client.getSync("checkpoint/" + hash).body.x[Option[CheckpointBlock]]

              if (cbo.nonEmpty) {
                val cb = cbo.get
                val cbCache = (dao.dbActor ? DBGet(cb.baseHash)).mapTo[Option[CheckpointCacheData]].get()

                if (cbCache.isEmpty && cb != dao.genesisObservation.get.initialDistribution &&
                  cb != dao.genesisObservation.get.initialDistribution2) {

                  EdgeProcessor.acceptCheckpoint(cb)
                  dao.metricsManager ! IncrementMetric("downloadedBlocks")

                  cb.parentSOEBaseHashes.foreach {
                    parentsAwaitingDownload.add
                  }
                }
              }
            }
          } else {
            threadsFinished(i) = true
          }

        } while (threadsFinished.exists(_._2 == false))

      }
    }

    Future.sequence(downloadResult).get(100000)

  //  Thread.sleep(20*1000)

   // dao.metricsManager ! UpdateMetric("downloadedBlocks", count.toString)
    //dao.metricsManager ! UpdateMetric("downloadedBlocksMemSize", cbs.size.toString)


// Solution #2 also has issues.
    val parentsAwaitingDownload = new ConcurrentLinkedQueue[String]()
    val threadsFinished = TrieMap[Int, Boolean]()

    activeTips.foreach{ z =>
      EdgeProcessor.acceptCheckpoint(z)
      z.parentSOEBaseHashes.foreach{h =>
        parentsAwaitingDownload.add(h)
      }
    }

    val par = Seq.fill(100)(0)
    val downloadResult = par.map{ i =>

      Future {
        threadsFinished(i) = false

        do {

          val hash = parentsAwaitingDownload.poll()
          if (hash != null) {
            threadsFinished(i) = false
            val soeCache = (dao.dbActor ? DBGet(hash)).mapTo[Option[SignedObservationEdgeCache]].get()
            if (soeCache.isEmpty) {

/* // TODO: Investigate bug here with ECs

              val askRes = dao.peerManager ? APIBroadcast(_.get("checkpoint/" + hash).map {
                _.body.x[Option[CheckpointBlock]]
              }, peerSubset = Set(id))

              val result = askRes.mapTo[Map[Id, Future[Option[CheckpointBlock]]]].flatMap {_.head._2}
              val cbo = result.get()
*/

              val cbo = singlePeer.client.getSync("checkpoint/" + hash).body.x[Option[CheckpointBlock]]

              if (cbo.nonEmpty) {
                val cb = cbo.get
                val cbCache = (dao.dbActor ? DBGet(cb.baseHash)).mapTo[Option[CheckpointCacheData]].get()

                if (cbCache.isEmpty && cb != dao.genesisObservation.get.initialDistribution &&
                  cb != dao.genesisObservation.get.initialDistribution2) {

                  EdgeProcessor.acceptCheckpoint(cb)
                  count += 1
                  if (count % 100 == 0) {
                    dao.metricsManager ! UpdateMetric("downloadedBlocks", count.toString)
                  }
                  cb.parentSOEBaseHashes.foreach {
                    parentsAwaitingDownload.add
                  }
                }
              }
            }
          } else {
            threadsFinished(i) = true
          }

        } while (threadsFinished.exists(_._2 == false))

      }
    }



// This has something to do with the API dispatcher EC vs fixedthreadpool, not the futures I think
// TODO: Debug this to fix, faster but executes out of order?
      def processAncestor(h: String): Future[Unit] = {
        if (!cbs.contains(h)) {
          val res =
            (dao.peerManager ? APIBroadcast(_.get("checkpoint/" + h).map {
              _.body.x[Option[CheckpointBlock]]
            }(ec)))
              .mapTo[Map[Id, Future[Option[CheckpointBlock]]]].flatMap {
              _.head._2
            }

          res.map {
            _.map { cb =>
              if (!cbs.contains(cb.baseHash)) {
                cbs(cb.baseHash) = cb
                count += 1
                if (count % 100 == 0) {
                  dao.metricsManager ! UpdateMetric("downloadedBlocks", count.toString)
                }
                if (!cb.parentSOE.contains(null)) {
                  Future.sequence(cb.parentSOEBaseHashes.map(processAncestor))
                } else Future.unit
              } else Future.unit
            }.getOrElse(Future.unit)
          }
        } else Future.unit
      }

      val done = activeTips.par.map { z =>
        cbs(z.baseHash) = z
        z.parentSOEBaseHashes.par.map {
          processAncestor
        }
      }

      val wait = Future.sequence(done.toList.flatMap {
        _.toList
      })

      wait.get(600)
 */


// Previous download code
trait Download extends PeerAuth {

  val data: DAO
  import data._
  // TODO: update since heartbeat is gone
  def downloadHeartbeat(): Unit = {
    if (downloadInProgress || !downloadMode || peers.isEmpty) return

    logger.debug("Requesting data download")

    downloadInProgress = true

    val apiClient = new APIClient()

    // get max bundle and genesis hash
    val maxBundleResponse = getMaxBundleHash(apiClient)

    val apiAddress = maxBundleResponse._1.get

    apiClient.setConnection(apiAddress.getHostName, apiAddress.getPort)

    val maxBundleSheaf = maxBundleResponse._2.get.sheaf.get
    val maxBundleHash = maxBundleSheaf.bundle.hash
    val genesisBundle = maxBundleResponse._2.get.genesisBundle.get

    val genesisTx = maxBundleResponse._2.get.genesisTX.get

    // update our genesis bundle
    //  acceptGenesis(genesisBundle, genesisTx)

    val pendingChainHashes = getPendingChainHashes(maxBundleHash, genesisBundle.hash, apiClient)

    val peerSelection = Iterator.continually(peers).flatten

    resolveChain(pendingChainHashes, peerSelection, apiClient)
  }

  def isChainFullyResolved(pendingChainHashes: mutable.LinkedHashMap[String, Boolean]): Boolean = {
    !pendingChainHashes.values.toSet(false)
  }

  def resolveChain(pendingChainHashes: mutable.LinkedHashMap[String, Boolean],
                   peerSelection: Iterator[Signed[Peer]],
                   apiClient: APIClient): Unit = {

    // TODO: add retry for missing bundles
    resolveBundleData(pendingChainHashes, peerSelection, apiClient).foreach(f => {
      f.onComplete(r => {
        // check if we are finished downloading
        if (isChainFullyResolved(pendingChainHashes)) {
          // turn off download mode
          downloadMode = false
          downloadInProgress = false
        }
      })
    })
  }

  def resolveBundleData(pendingChainHashes: mutable.LinkedHashMap[String, Boolean],
                        peerSelection: Iterator[Signed[Peer]],
                        apiClient: APIClient): Seq[Future[Unit]] = {

    val bundleResponses = getPartitionedPendingChainHashes(pendingChainHashes).flatMap(group => {
      val client = getRandomPeerClientConnection(peerSelection)

      group.map(bundle => {
        client.get("fullBundle/" + bundle._1)
      })
    }).toSeq

    bundleResponses.map(response => {
      response.map(r => {
        if (r.isSuccess) {
          val response = apiClient.readHttpResponseEntity[BundleHashQueryResponse](r.body)

          if (response.sheaf.isDefined) {
            val sheaf: Sheaf = response.sheaf.get

            val transactions: Seq[Transaction] = response.transactions

            // store the bundle
            handleBundle(sheaf.bundle)

            // store the transactions
            // TODO: update for latest
            //  transactions.foreach(handleTransaction)

            // set the bundle to be non pending
            pendingChainHashes(sheaf.bundle.hash) = true
          }

        } else {
          logger.debug(s"fetch bundle failure ")
        }
      })
    })
  }

  def getPendingChainHashes(maxBundleHash: String,
                            genesisBundleHash: String,
                            apiClient: APIClient): mutable.LinkedHashMap[String, Boolean] = {
    val pendingChainHashes = mutable.LinkedHashMap[String, Boolean](maxBundleHash -> false)

    var hash = maxBundleHash

    // grab all of the chain hashes
    while (hash != genesisBundleHash) {
      val ancestors = apiClient.get("ancestors/" + hash)

      val response = Await.ready(ancestors, 90 seconds)

      val ancestorHashes = apiClient.read[Seq[String]](response.get())

      hash = ancestorHashes.head

      ancestorHashes.foreach(h => {
        pendingChainHashes.+=(h -> false)
      })
    }

    pendingChainHashes
  }

  def getPartitionedPendingChainHashes(pendingChainHashes: mutable.LinkedHashMap[String, Boolean]):
  Iterator[mutable.LinkedHashMap[String, Boolean]] = {

    val chainHashes = pendingChainHashes.filterNot(p => p._2)

    // split out work and request bundles and transactions for all of the chain hashes
    val groupedChain = if (chainHashes.size > peers.size) {
      chainHashes.grouped(chainHashes.size / peers.size)
    } else {
      chainHashes.grouped(peers.size)
    }

    groupedChain
  }

  def getRandomPeerClientConnection(peerSelection: Iterator[Signed[Peer]]): APIClient = {
    val peer = peerSelection.next().data.apiAddress.get

    val client = new APIClient().setConnection(peer.getHostName, peer.getPort)

    client
  }

  def getMaxBundleHash(apiClient: APIClient): (Option[InetSocketAddress], Option[MaxBundleGenesisHashQueryResponse]) = {
    val maxBundles: Seq[(InetSocketAddress, Future[HttpResponse[String]])] = getBroadcastTCP(route = "maxBundle")

    val futures = Future.sequence(maxBundles.map(b => b._2))

    Await.ready(futures, 90 seconds)

    val maxBundle = maxBundles.foldLeft[(Option[InetSocketAddress],
      Option[MaxBundleGenesisHashQueryResponse])]((None, None))((acc, f) => {

      val right = f._2.get()

      val leftHash: Option[Sheaf] = if (acc._2.isDefined) acc._2.get.sheaf else None
      val rightHash = apiClient.read[Option[MaxBundleGenesisHashQueryResponse]](right)

      if (leftHash.isDefined && rightHash.isDefined && rightHash.get.sheaf.isDefined) {
        val leftHashScore = leftHash.get.totalScore.get

        val rightHashScore = rightHash.get.sheaf.get.totalScore.get

        if (leftHashScore > rightHashScore) {
          acc
        } else {
          (Some(f._1), rightHash)
        }
      } else if (leftHash.isDefined) {
        acc
      } else {
        (Some(f._1), rightHash)
      }
    })

    maxBundle
  }

}
