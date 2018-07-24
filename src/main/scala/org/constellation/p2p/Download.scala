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
    if (downloadMode && peers.nonEmpty && !downloadInProgress) {
      logger.debug("Requesting data download")

      downloadInProgress = true

      val apiClient = new APIClient()

      // call to get ancestors from each peer
      // grab the one with the highest score?
      // take the list of ancestors, split out work to grab bundle and transaction data from each ancestor bundle,
      // once we get to the end, hit another endpoint to get the same list but from the new bundle hash
      // once we get to coinbase return

      // grab max bundle hash

      getMaxBundleHash(apiClient).onComplete(bundleHash => {

        // TODO: use disk backing
        var validSheafs = Seq[Sheaf]()
        var validTransactions = Seq[Transaction]()

        var hash = bundleHash.get

        var chain = Seq[String]()

        while (hash != "coinbase") {
          val peer = getBroadcastPeers().head
          val apiAddress = peer.apiAddress.get

          apiClient.setConnection(apiAddress.getHostName, apiAddress.getPort)

          val ancestors = apiClient.get("ancestors")

          ancestors.onComplete(a => {
            val thing = apiClient.read[Seq[String]](a.get).get()
            hash = thing.last
            chain = chain.++(thing)
          })
        }

        assert(true)
    /*
        while (hash != "coinbase") {

          // get bundle data

          ///// **
          val sheaf: Seq[Future[HttpResponse]] = getBroadcastTCP(route = "bundle/" + hash)

          val downloadResponse = Future.sequence(sheaf)

          Await.ready(downloadResponse, 30 seconds).onComplete(r => {
            val responses = r.get

            val sheafs = responses.map(r => {
              val sheaf = apiClient.read[Option[Sheaf]](r).get()
              sheaf
            }).toSeq

            if (sheafs.nonEmpty) {
              val sheaf = sheafs.filter(_.isDefined).minBy(_.get.totalScore).get

              handleBundle(sheaf.bundle)

              hash = sheaf.bundle.extractParentBundleHash.pbHash

              validSheafs = validSheafs :+ sheaf

              ////// **

              /// get transaction data
              ////// ***
              val peers = getBroadcastPeers()

              val transactions = sheaf.bundle.extractTXHash

              val groupedTransactions = if (transactions.size > peers.size) {
                transactions.grouped(transactions.size / peers.size)
              } else {
                transactions.grouped(peers.size)
              }

              val peerSelection = Iterator.continually(peers).flatten

              val transactionResponses = groupedTransactions.flatMap(group => {
                val peer = peerSelection.next().apiAddress.get

                val client = new APIClient().setConnection(peer.getHostName, peer.getPort)

                group.map(tx => {
                  client.get("transaction/" + tx.txHash)
                })
              }).toSeq

              val downloadResponse = Future.sequence(transactionResponses)

              Await.ready(downloadResponse, 30 seconds).onComplete(f => {

                if (f.get.nonEmpty) {
                  val sheaf = apiClient.read[Option[Transaction]](f.get.head).get()

                  if (sheaf.isDefined) {
                    validTransactions = validTransactions :+ sheaf.get
                  }
                }
              })
            }

            ///// ***

            sheafs
          })
        }

        */

        if (hash == "coinbase") {
          acceptGenesis(validSheafs.last.bundle, validSheafs.last.bundle.extractTX.head)
        }

        // turn off download mode
        downloadMode = false
        downloadInProgress = false
      })

    }
  }

  def getMaxBundleHash(apiClient: APIClient): Future[String] = {
    val maxBundles: Seq[(InetSocketAddress, Future[HttpResponse])] = getBroadcastTCP(route = "maxBundle")

    val reduced = Future.reduce(maxBundles)((left, right) => {
      val leftHash = apiClient.read[Option[Sheaf]](left).get()
      val rightHash = apiClient.read[Option[Sheaf]](right).get()

      if (leftHash.isDefined && rightHash.isDefined) {
        val leftHashScore = leftHash.get.totalScore.get

        val rightHashScore = rightHash.get.totalScore.get

        if (leftHashScore > rightHashScore) left else right
      } else if (leftHash.isDefined) {
        left
      } else {
        right
      }
    })

    reduced.collect {
      case response: HttpResponse => {
        apiClient.read[Option[Sheaf]](response).get().get.bundle.hash
      }
    }
  }

}
