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
    if (d.validTX.nonEmpty && d.validUTXO.nonEmpty) {
      validTX = d.validTX
      validTX.filter{_.tx.data.isGenesis}.foreach{
        z =>
          genesisTXHash = z.hash
      }
      d.validUTXO.foreach{ case (k,v) =>
        validLedger(k) = v
        memPoolLedger(k) = v
      }
      downloadMode = false
      logger.debug("Downloaded data")
    }
  }

  def handleDownloadRequest(d: DownloadRequest, remote: InetSocketAddress): Unit = {
    val downloadResponse = DownloadResponse(validTX, validLedger.toMap)
    udpActor.udpSend(downloadResponse, remote)
  }

}
