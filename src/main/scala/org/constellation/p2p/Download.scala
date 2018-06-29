package org.constellation.p2p

import java.net.InetSocketAddress

import org.constellation.Data
import org.constellation.primitives.Schema.{DownloadRequest, DownloadResponse}
import constellation._

trait Download extends PeerAuth {

  val data: Data
  import data._

  def downloadHeartbeat(): Unit = {
    if (downloadMode && peers.nonEmpty && !downloadInProgress) {
      logger.debug("Requesting data download")
      broadcast(DownloadRequest())
    }
  }

  def handleDownloadResponse(d: DownloadResponse): Unit = {
    if (genesisBundle == null) {
      db.put(d.genesisTX)
      acceptGenesis(d.genesisBundle)
      if (d.bestBundle.hash == d.genesisBundle.hash) {
        downloadMode = false
      } else {
        handleBundle(d.bestBundle)
        downloadInProgress = true
      }
      //   lastCheckpointBundle = d.lastCheckpointBundle
      logger.debug("Downloaded data")
    }
  }

  def handleDownloadRequest(d: DownloadRequest, remote: InetSocketAddress): Unit = {
    if (genesisBundle != null && !downloadMode) {
      logger.debug("Sending download response")
      val downloadResponse = DownloadResponse(genesisBundle, genesisBundle, genesisBundle.extractTX.head )
      udpActor.udpSend(downloadResponse, remote)
    }
  }

}
