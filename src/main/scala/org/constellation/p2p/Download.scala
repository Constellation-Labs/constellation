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
    if (genesisBundle.isEmpty) {
      acceptGenesis(d.genesisBundle, d.genesisTX)
      if (d.maxBundle.hash == d.genesisBundle.hash) {
        downloadMode = false
      } else {
        handleBundle(d.maxBundle)
        downloadInProgress = true
        downloadMode = false
      }
      //   lastCheckpointBundle = d.lastCheckpointBundle
      logger.debug("Downloaded data")
    }
  }

  def handleDownloadRequest(d: DownloadRequest, remote: InetSocketAddress): Unit = {
    if (genesisBundle.nonEmpty && !downloadMode && !downloadInProgress && maxBundle.nonEmpty) {
      logger.debug("Sending download response")
      val downloadResponse = DownloadResponse(maxBundle.get, genesisBundle.get, genesisBundle.get.extractTX.head )
      udpActor ! UDPSend(downloadResponse, remote)
    }
  }

}
