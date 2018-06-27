package org.constellation.p2p

import java.net.InetSocketAddress

import org.constellation.Data
import org.constellation.primitives.Schema.{DownloadRequest, DownloadResponse}
import constellation._

trait Download extends PeerAuth {

  val data: Data
  import data._

  def downloadHeartbeat(): Unit = {
    if (downloadMode && peers.nonEmpty) {
      logger.debug("Requesting data download")
      broadcast(DownloadRequest())
    }
  }

  def handleDownloadResponse(d: DownloadResponse): Unit = {
    if (d.validBundles.nonEmpty) {
      genesisBundle = d.genesisBundle
      processNewBundleMetadata(genesisBundle, genesisBundle.extractTX, true, setActive = false)
      d.validBundles.tail.foreach { b =>
        processNewBundleMetadata(b, b.extractTX)
      }
      validBundles = d.validBundles
      d.ledger.foreach{ case (k,v) =>
          validLedger(k) = v
          memPoolLedger(k) = v
      }

      validTX ++= validBundles.flatMap(_.extractTX).toSet
      downloadMode = false

      lastCheckpointBundle = d.lastCheckpointBundle

      logger.debug("Downloaded data")
    }
  }

  def handleDownloadRequest(d: DownloadRequest, remote: InetSocketAddress): Unit = {
    if (genesisBundle != null && validBundles.nonEmpty) {
      val downloadResponse = DownloadResponse(genesisBundle, validBundles, validLedger.toMap, lastCheckpointBundle)
      udpActor.udpSend(downloadResponse, remote)
    }
  }

}
