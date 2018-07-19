package org.constellation.p2p

import java.net.InetSocketAddress

import akka.http.scaladsl.model.{HttpResponse, StatusCode}
import org.constellation.Data
import org.constellation.primitives.Schema.{BundleHashQueryResponse, DownloadRequest, DownloadResponse}
import constellation._
import org.constellation.serializer.KryoSerializer
import org.constellation.util.APIClient

import scala.concurrent.{Await, Future}

trait Download extends PeerAuth {

  val data: Data
  import data._

  def downloadHeartbeat(): Unit = {
    if (downloadMode && peers.nonEmpty && !downloadInProgress) {
      logger.debug("Requesting data download")

      // ask each peer for download
      // once we have more than a threshold
      // take chain

      // acceptGenesis(d.genesisBundle, d.genesisTX)
      // handleBundle(d.maxBundle)
      // downloadInProgress = true
      // downloadMode = false
      // DownloadResponse
      // DownloadRequest

      val download: Seq[Future[HttpResponse]] = getBroadcastTCP(route = "download")

      val downloadResponse = Future.sequence(download)

      downloadResponse.onComplete(r => {
        val responses = r.get

        val chains = responses.map(r => {
          r.entity.getDataBytes().map(f => {
            val chain = KryoSerializer.deserialize(f.toArray).asInstanceOf[Seq[BundleHashQueryResponse]]
            chain
          })
        })

        logger.debug(s"download complete chains = $chains")
        // TODO: temp
      })

    }

  }

}
