package org.constellation.util

import com.softwaremill.macmemo.memoize
import constellation._
import org.constellation.DAO
import org.constellation.crypto.SimpleWalletLike
import org.constellation.primitives.Schema.MetricsResult

import scala.concurrent.duration._

class Metrics(val dao: DAO = null) extends SimpleWalletLike {

  import dao._

  @memoize(maxSize = 1, expiresAfter = 2.seconds)
  def calculateMetrics(): MetricsResult = {
    MetricsResult(
      Map(
        "transactionsPerSecond" -> transactionsPerSecond.toString,
        "version" -> "1.0.2",
        "allPeersHealthy" -> allPeersHealthy.toString,
        //   "numAPICalls" -> numAPICalls.toString,
        "numMempoolEmits" -> numMempoolEmits.toString,
        "numDBGets" -> numDBGets.toString,
        "numDBPuts" -> numDBPuts.toString,
        "numDBDeletes" -> numDBDeletes.toString,
        "numTXRemovedFromMemory" -> numTXRemovedFromMemory.toString,
        "numDeletedBundles" -> numDeletedBundles.toString,
        "numValidBundleHashesRemovedFromMemory" -> numValidBundleHashesRemovedFromMemory.toString,
        "udpPacketGroupSize" -> udpPacketGroupSize.toString,
        "address" -> selfAddress.address,
/*        "balance" -> (selfIdBalance
          .getOrElse(0L) / Schema.NormalizationFactor).toString,*/
        "id" -> id.b58,
        "z_keyPair" -> keyPair.json,
        "shortId" -> id.short,
        "numSyncedTX" -> numSyncedTX.toString,
        "numP2PMessages" -> totalNumP2PMessages.toString,
        "numSyncedBundles" -> numSyncedBundles.toString,
        "numValidBundles" -> totalNumValidBundles.toString,
        "numValidTransactions" -> totalNumValidatedTX.toString,
        "totalNumBroadcasts" -> totalNumBroadcastMessages.toString,
        "totalNumBundleMessages" -> totalNumBundleMessages.toString,
        "lastConfirmationUpdateTime" -> lastConfirmationUpdateTime.toString,
        "numPeers" -> peers.size.toString,
        "peers" -> peers
          .map { z =>
            val addr = z.data.apiAddress.map {
              a => s"http://${a.getHostString}:${a.getPort}"
            }.getOrElse("")
            s"${z.data.id.short} API: $addr"
          }
          .mkString(" --- "),
        "z_peerLookup" -> signedPeerLookup.toMap.toString,
        "downloadInProgress" -> downloadInProgress.toString,
        "reputations" -> normalizedDeterministicReputation
          .map {
            case (k, v) => k.short + " " + v
          }
          .mkString(" - "),
        "peersAwaitingAuthentication" -> peersAwaitingAuthenticationToNumAttempts.toMap
          .toString(),
        "numProcessedBundles" -> totalNumNewBundleAdditions.toString,
        "z_peers" -> peers.map {
          _.data
        }.json,
        "downloadMode" -> downloadMode.toString
      ))
  }
}
