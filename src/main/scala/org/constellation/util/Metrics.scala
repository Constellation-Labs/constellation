package org.constellation.util

import com.softwaremill.macmemo.memoize
import constellation._
import org.constellation.DAO
import org.constellation.crypto.SimpleWalletLike
import org.constellation.primitives.Schema.MetricsResult

import scala.concurrent.duration._
import scala.util.Try

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
        "numSheafInMemory" -> bundleToSheaf.size.toString,
        "numValidBundleHashesRemovedFromMemory" -> numValidBundleHashesRemovedFromMemory.toString,
        "udpPacketGroupSize" -> udpPacketGroupSize.toString,
        "address" -> selfAddress.address,
/*        "balance" -> (selfIdBalance
          .getOrElse(0L) / Schema.NormalizationFactor).toString,*/
        "id" -> id.b58,
        "z_keyPair" -> keyPair.json,
        "shortId" -> id.short,
        "last1000BundleHashSize" -> last100ValidBundleMetaData.size.toString,
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
        "z_peerSync" -> peerSync.toMap.toString,
        "z_peerLookup" -> signedPeerLookup.toMap.toString,
        "downloadInProgress" -> downloadInProgress.toString,
        "z_genesisBundleHash" -> genesisBundle
          .map {
            _.hash
          }
          .getOrElse("N/A"),
        //   "bestBundleCandidateHashes" -> bestBundleCandidateHashes.map{_.hash}.mkString(","),
      /*  "numActiveBundles" -> activeDAGBundles.size.toString,*/
        "last10ValidBundleHashes" -> last100ValidBundleMetaData
          .map {
            _.bundle.hash
          }
          .takeRight(10)
          .mkString(","),
        "lastValidBundleHash" -> Try {
          lastValidBundleHash.pbHash
        }.getOrElse(""),
        "lastValidBundle" -> Try {
          Option(lastValidBundle)
            .map {
              _.pretty
            }
            .getOrElse("")
        }.getOrElse(""),
        "z_genesisBundle" -> genesisBundle.map(_.json).getOrElse(""),
        "z_genesisBundleIds" -> genesisBundle
          .map(_.extractIds.mkString(", "))
          .getOrElse(""),
        "selfBestBundle" -> Try {
          maxBundle.map {
            _.pretty
          }.toString
        }.getOrElse(""),
        "selfBestBundleHash" -> Try {
          maxBundle.map {
            _.hash
          }.toString
        }.getOrElse(""),
        "selfBestBundleMeta" -> Try {
          maxBundleMetaData.toString
        }.getOrElse(""),
        "reputations" -> normalizedDeterministicReputation
          .map {
            case (k, v) => k.short + " " + v
          }
          .mkString(" - "),
        "peersAwaitingAuthentication" -> peersAwaitingAuthenticationToNumAttempts.toMap
          .toString(),
        "numProcessedBundles" -> totalNumNewBundleAdditions.toString,
        "numSyncPendingBundles" -> syncPendingBundleHashes.size.toString,
        "numSyncPendingTX" -> syncPendingTXHashes.size.toString,
        "peerBestBundles" -> peerSync.toMap
          .map {
            case (id, b) =>
              Try {
                s"${id.short}: ${b.maxBundle.hash.take(5)} ${Try {
                  b.maxBundle.pretty
                }} " +
                  s"parent${b.maxBundle.extractParentBundleHash.pbHash.take(5)} " +
                  s"${lookupBundle(b.maxBundle.hash).nonEmpty} ${b.maxBundle.meta.map {
                    _.transactionsResolved
                  }}"
              }.getOrElse("")
          }
          .mkString(" --- "),
        "z_peers" -> peers.map {
          _.data
        }.json,
        "downloadMode" -> downloadMode.toString,
        "allPeersHaveKnownBestBundles" -> Try {
          peerSync.forall {
            case (_, hb) =>
              lookupBundle(hb.maxBundle.hash).nonEmpty
          }.toString
        }.getOrElse(""),
    /*    "allPeersAgreeOnValidLedger" -> Try {
          peerSync.forall {
            case (_, hb) =>
              hb.validLedger == validLedger.toMap
          }.toString
        }.getOrElse(""),*/
        "allPeersHaveResolvedMaxBundles" -> Try {
          peerSync.forall {
            _._2.safeMaxBundle.exists {
              _.meta.exists(_.isResolved)
            }
          }.toString
        }.getOrElse(""),
        "allPeersAgreeWithMaxBundle" -> Try {
          peerSync.forall {
            _._2.maxBundle == maxBundle.get
          }.toString
        }.getOrElse("")
        //,
        // "z_lastBundleVisualJSON" -> Option(lastBundle).map{ b => b.extractTreeVisual.json}.getOrElse("")
      ))
  }
}
