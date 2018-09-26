package org.constellation.p2p

import java.net.InetSocketAddress

import constellation._
import org.constellation.Data
import org.constellation.primitives.Schema._
import org.constellation.util.{APIClient, Signed}
import scalaj.http.HttpResponse

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

trait Download extends PeerAuth {

  val data: Data
  import data._

  // TODO: update since heartbeat is gone
  def downloadHeartbeat(): Unit = {
    if (downloadInProgress || !downloadMode || peers.isEmpty) return

    logger.debug("Requesting data download")

    downloadInProgress = true

    val client = APIClient(port = 9000)

    // get max bundle and genesis hash
    val maxBundleResponse = getMaxBundleHash(client)

    val apiAddress = maxBundleResponse._1.get

    val apiClient = APIClient(apiAddress.getHostName, apiAddress.getPort)

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

    val client = APIClient(peer.getHostName, peer.getPort)

    client
  }

  def getMaxBundleHash(apiClient: APIClient): (Option[InetSocketAddress], Option[MaxBundleGenesisHashQueryResponse]) = {
    // TODO: get rid of apiClient here -- it's only used to read and deserialize the HTTPResponse from the broadcast.
    // Must be a better way
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
