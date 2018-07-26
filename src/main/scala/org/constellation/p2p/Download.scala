package org.constellation.p2p

import java.net.InetSocketAddress

import akka.http.scaladsl.model.{HttpResponse, ResponseEntity, StatusCode}
import org.constellation.Data
import org.constellation.primitives.Schema._
import constellation._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.APIClient

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

trait Download extends PeerAuth {

  val data: Data
  import data._

  def downloadHeartbeat(): Unit = {
    if (downloadInProgress || !downloadMode || peers.isEmpty) {
      return ()
    }

    logger.debug("Requesting data download")

    downloadInProgress = true

    val apiClient = new APIClient()

    // get max bundle and genesis hash
    val maxBundle = getMaxBundleHash(apiClient)

    val apiAddress = maxBundle._1.get

    apiClient.setConnection(apiAddress.getHostName, apiAddress.getPort)

    var hash = maxBundle._2.get.sheaf.get.bundle.hash
    val genesisHash = maxBundle._2.get.genesisHash

    var pendingChainHashes = Set[String](hash)

    // grab all of the chain hashes
    while (hash != genesisHash) {
      val ancestors = apiClient.get("ancestors/" + hash)

      val response = Await.ready(ancestors, 90 seconds)

      val thing = apiClient.read[Seq[String]](response.get()).get()

      hash = thing.head
      pendingChainHashes = pendingChainHashes.++(thing)
    }

    // split out work and request bundles and transactions for all of the chain hashes
    val groupedChain = if (pendingChainHashes.size > peers.size) {
      pendingChainHashes.grouped(pendingChainHashes.size / peers.size)
    } else {
      pendingChainHashes.grouped(peers.size)
    }

    val peerSelection = Iterator.continually(peers).flatten

    val bundleResponses = groupedChain.flatMap(group => {
      val peer = peerSelection.next().data.apiAddress.get

      val client = new APIClient().setConnection(peer.getHostName, peer.getPort)

      group.map(bundle => {
        client.get("fullBundle/" + bundle)
      })
    }).toSeq

    bundleResponses.foreach(f => {
      f.onComplete(r => {
        if (r.isSuccess) {
          val response = apiClient.read[BundleHashQueryResponse](r.get).get()

          val sheaf: Sheaf = response.sheaf.get

          handleBundle(sheaf.bundle)

          // If this is the genesis bundle handle it separately
          if (sheaf.bundle.hash == genesisHash) {
            acceptGenesis(sheaf.bundle, sheaf.bundle.extractTX.head)
          }

          pendingChainHashes -= sheaf.bundle.hash

          // check if we are finished downloading
          if (pendingChainHashes.isEmpty) {
            // turn off download mode
            downloadMode = false
            downloadInProgress = false
          }

        } else {
          logger.debug(s"fetch bundle failure ")
        }
      })
    })

  }

  def getMaxBundleHash(apiClient: APIClient): (Option[InetSocketAddress], Option[MaxBundleGenesisHashQueryResponse]) = {
    val maxBundles: Seq[(InetSocketAddress, Future[HttpResponse])] = getBroadcastTCP(route = "maxBundle")

    val futures = Future.sequence(maxBundles.map(b => b._2))

    Await.ready(futures, 90 seconds)

    val maxBundle = maxBundles.foldLeft[(Option[InetSocketAddress],
      Option[MaxBundleGenesisHashQueryResponse])]((None, None))((acc, f) => {

      val right = f._2.get()

      val leftHash: Option[Sheaf] = if (acc._2.isDefined) acc._2.get.sheaf else None
      val rightHash = apiClient.read[Option[MaxBundleGenesisHashQueryResponse]](right).get()

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
