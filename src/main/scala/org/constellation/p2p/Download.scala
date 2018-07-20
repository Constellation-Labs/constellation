package org.constellation.p2p

import java.net.InetSocketAddress

import akka.http.scaladsl.model.{HttpResponse, StatusCode}
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
    if (downloadMode && peers.nonEmpty && !downloadInProgress) {
      logger.debug("Requesting data download")

      val maxBundles: Seq[Future[HttpResponse]] = getBroadcastTCP(route = "maxBundle")

      val future = Future.sequence(maxBundles)

      future.onComplete(f => {
        val responses = f.get.seq.map(f => {
          val hash = new APIClient("localhost", 8080).read[Option[Sheaf]](f).get()
          hash
        }).filter(_.isDefined).sortBy(_.get.height.get)

        if (responses.nonEmpty) {
          var hash = responses.head.get.bundle.hash

          var validSheafs = Seq[Sheaf]()
          var validTransactions = Seq[Transaction]()

          while (hash != "coinbase") {
            val sheaf: Seq[Future[HttpResponse]] = getBroadcastTCP(route = "bundle/" + hash)

            val downloadResponse = Future.sequence(sheaf)

            Await.ready(downloadResponse, 30 seconds).onComplete(r => {
              val responses = r.get

              val sheafs = responses.map(r => {
                val sheaf = new APIClient("localhost", 8080).read[Option[Sheaf]](r).get()
                sheaf
              }).toSeq

              if (sheafs.nonEmpty) {
                val sheaf = sheafs.head.get

                handleBundle(sheaf.bundle)

                hash = sheaf.bundle.extractParentBundleHash.pbHash

                validSheafs = validSheafs :+ sheaf

                sheaf.bundle.extractTXHash.foreach{ h =>
                  val hActual = h.txHash
                  val transactions: Seq[Future[HttpResponse]] = getBroadcastTCP(route = "transaction/" + hActual)

                  val downloadResponse = Future.sequence(transactions)

                  Await.ready(downloadResponse, 30 seconds).onComplete(f => {

                    if (f.get.nonEmpty) {
                      val sheaf = new APIClient("localhost", 8080).read[Option[Transaction]](f.get.head).get()

                      if (sheaf.isDefined) {
                        validTransactions = validTransactions :+ sheaf.get
                      }
                    }
                  })
                }
              }

              sheafs
            })
          }

          if (hash == "coinbase") {
            acceptGenesis(validSheafs.last.bundle, validSheafs.last.bundle.extractTX.head)
          }

          // turn off download mode
          downloadMode = false
          downloadInProgress = false
        }
      })

    }

  }

}
