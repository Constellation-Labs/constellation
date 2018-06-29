package org.constellation.p2p

import java.net.InetSocketAddress

import org.constellation.Data
import org.constellation.primitives.Schema._
import org.constellation.util.ProductHash
import constellation._

import scala.collection.concurrent.TrieMap
import scala.util.{Random, Try}

trait ProbabilisticGossip extends PeerAuth with LinearGossip {

  val data: Data

  import data._

  def gossipHeartbeat(): Int = {
    heartbeatRound += 1
    if (!downloadMode) {
      bundleHeartbeat()
    }
    0

  }
  def acceptBundle(b: Bundle): Unit = {
    // validBundles :+= b
    // calculateReputationsFromScratch()
    totalNumValidBundles += 1
    last100BundleHashes :+= b.hash
    b.extractTX.foreach{ t =>
      acceptTransaction(t)
    }
    // activeDAGBundles = Seq()
  }


  def genesisCheck(): Unit = {

    // First node who did genesis
    val bbExist = Option(maxBundle).exists {_.txBelow.size >= minGenesisDistrSize - 1}
    if ( genesisAdditionCheck && bbExist) acceptBundle(maxBundle)

    // Other peers
    if (lastValidBundleHash.pbHash == genesisBundle.hash && !(genesisBundle.extractIds.head == id)) {
      peerSync.get(genesisBundle.extractIds.head).foreach { b =>
        if (b.validBundleHashes.size == 2) {
          bundleHashToBundle.get(b.validBundleHashes.last).foreach { vb =>
            acceptBundle(vb)
          }
        }
      }
    }
  }

  def genesisAdditionCheck: Boolean = Try {
    last100BundleHashes.size == 1 &&
      genesisBundle.extractIds.head == id // This node created the genesis bundle
  }.getOrElse(false)

  def extractTXHashUntilValidationHashReached(b: Bundle, txs: Set[String] = Set()): Set[String] = {
    val ph = b.extractParentBundleHash.pbHash
    if (last100BundleHashes.last == ph) {
      txs ++ b.txBelow
    } else {
      // if (!bundleHashToBundle.contains(ph)) {
      //   syncPendingBundleHashes += ph
      // bundleHashToBundle.remove(b.hash)
      //  activeDAGBundles = activeDAGBundles.filter{_ != b}
      //  Set[String]()
      //} else
      extractTXHashUntilValidationHashReached(
        bundleHashToBundle(b.extractParentBundleHash.pbHash),
        txs ++ b.txBelow
      )}
  }

  def randomTransaction(): Unit = {
    val peerAddresses = peers.map{_.data.id.address}
    val randomPeer = Random.shuffle(peerAddresses).head
    createTransaction(randomPeer.address, Random.nextInt(1000).toLong)
  }

  def simulateTransactions(): Unit = {
    if (last100BundleHashes.size >= 5) {
      if (memPool.size < 50) {
        Seq.fill(3){randomTransaction()}
      } else {
        if (Random.nextDouble() < .1) randomTransaction()
      }
    }
  }

  def bundleHeartbeat(): Unit = {

    if (syncPendingBundleHashes.nonEmpty) {
      broadcast(BatchBundleHashRequest(syncPendingBundleHashes))
    }
    if (syncPendingTXHashes.nonEmpty) {
      broadcast(BatchTXHashRequest(syncPendingTXHashes))
    }


    if (!downloadMode && genesisBundle != null) {

      simulateTransactions()

      if (memPool.size > 500) {
        memPool = memPool.slice(0, 250)
      }
      /*
            if (validBundles.size > 4 && (System.currentTimeMillis() > (lastConfirmationUpdateTime + 30000))) {
              activeDAGBundles = Seq()
            }*/


      if (totalNumValidBundles > 2) {
        activeDAGBundles = activeDAGBundles.filter {
          _.meta.height >= totalNumValidBundles - 1
        }
      }

      activeDAGBundles = activeDAGBundles.sortBy(z => (-1 * z.meta.totalScore, z.hash))

      val bb: Option[Bundle] = if (genesisAdditionCheck) activeDAGBundles.headOption else {
        activeDAGBundles.find{z => z.maxStackDepth >= 2 && z.meta.totalScore > maxBundle.meta.totalScore}
      }

      bb.foreach{maxBundle = _}


      if (maxBundle != null) {
        broadcast(
          PeerSyncHeartbeat(
            Some(maxBundle),
            last100BundleHashes
          )
        )
      }

      val txInBestBundleNewFromValidationHash = bb.map{b => b.extractTXHash.map{_.txHash}}.getOrElse(Set[String]())

      // || peers have no bundles / stalled.
      val memPoolEmit = Random.nextInt() < 0.2 // && (System.currentTimeMillis() < lastBundle.maxTime + 25000)
      val filteredMempool = memPool.diff(txInBestBundleNewFromValidationHash)

      def doMempool(): Unit = {
        // Emit an origin bundle. This needs to be managed by prob facil check on hash of previous + ids
        val memPoolSelSize = 5 + Random.nextInt(50)
        val memPoolSelection = Random.shuffle(filteredMempool.toSeq).slice(0, memPoolSelSize + minGenesisDistrSize + 1)
        val b = Bundle(
          BundleData(
            memPoolSelection.map{TransactionHash} :+ ParentBundleHash(maxBundle.hash)
          ).signed()
        )
        //     if (bb.nonEmpty) {
        //       activeDAGBundles = activeDAGBundles.filterNot(_ == bb.get)
        //     }
        processNewBundleMetadata(b, memPoolSelection.toSet.flatMap{m: String => db.getAs[TX](m)})
        broadcast(b)
      }

      if (totalNumValidBundles < 2) {
        if (genesisBundle.extractIds.head == id && activeDAGBundles.isEmpty && memPool.size >= minGenesisDistrSize - 1) doMempool()
      } else if (filteredMempool.nonEmpty && memPoolEmit) doMempool()


      genesisCheck()

      var toRemove: Set[Bundle] = Set()

      if (totalNumValidBundles >= 2) {
        //  if (System.currentTimeMillis() < (lastBundle.maxTime + 10000)) {
        val nonSelfIdCandidates = activeDAGBundles.filter { b => !b.idBelow.contains(id) }
        //   logger.debug(s"Num nonSelfIdCandidates ${id.short} ${nonSelfIdCandidates.size}")

        val grouped = nonSelfIdCandidates
          .groupBy(b => b.maxStackDepth -> b.extractParentBundleHash)

        grouped.foreach {
          case (_, bundles) =>
            //val sorted = bundles.sortBy(b => (b.idBelow.size, b.txBelow.size, b.hash))
            val random = Random.shuffle(bundles).slice(0, 5)
            if (bundles.size > 30) {
              //   toRemove = toRemove ++ Random.shuffle(bundles).slice(0, 10)
            }
            val iterator = random.combinations(2)
            iterator.foreach {
              case both@Seq(l, r) =>
                val hasNewTransactions = l.txBelow.diff(r.txBelow).nonEmpty
                val minTimeClose = Math.abs(l.minTime - r.minTime) < 40000
                if (Random.nextDouble() > 0.5 &&  hasNewTransactions && minTimeClose) { //
                  val b = Bundle(BundleData(both).signed())
                  processNewBundleMetadata(b, b.extractTX)
                  broadcast(b)
                  //  toRemove = toRemove ++ Set(l, r)
                  //      println(s"Created new depth bundle ${b.maxStackDepth}")
                }
            }
        }
      }


      def runParityCheck() = {
        db.getAs[Bundle](last100BundleHashes.last).filter{_ != "coinbase"}.foreach { lb =>
          val ancestors = extractBundleAncestorsUpTo(lb, Seq[String](), 100)
          val correct = ancestors.reverse.zip(last100BundleHashes.reverse.tail).forall {
            case (x, y) => x == y
          }
          println("PARITY CHECK " + correct)
        }
      }

      /*   if (heartbeatRound % 10 == 0 && totalNumValidBundles > 3) {
           runParityCheck()
         }
   */

      Option(maxBundle).filter{ _ => totalNumValidBundles >= 2}.foreach{ b =>

        val ancestors = extractBundleAncestorsUpTo(b, Seq[String](),20)

        if (ancestors.length > 8) {
          totalNumValidBundles = maxBundle.meta.height - 5
          val toAdd2 = ancestors.slice(0, ancestors.length - 5)
          toAdd2.map(bundleHashToBundle).flatMap(_.extractTX).foreach(z => acceptTransaction(z))
          val toAdd = toAdd2.filterNot(last100BundleHashes.contains)
          last100BundleHashes = toAdd
          if (toAdd.nonEmpty) {
            calculateReputationsFromScratch(totalNumValidBundles - 6)
            lastConfirmationUpdateTime = System.currentTimeMillis()
            activeDAGBundles = Seq()
          }
        }
        /*

                ancestors.headOption.foreach{ fa =>

                  val firstCommonAncestor = last1000BundleHashes
                    .zipWithIndex.collectFirst{case (x,y) if x == fa => y}
                    .filter{ ce =>
                      last1000BundleHashes
                        .slice(ce, last1000BundleHashes.size)
                        .zip(ancestors)
                        .forall{ case (z,w) => z == w} && ancestors.contains(last1000BundleHashes.last) &&
                      ancestors.length > 6
                    }

                  firstCommonAncestor.foreach{ ce =>

                    val newestToAdd = ancestors.zipWithIndex.find(_._1 == last1000BundleHashes.last).get._2 + 1
                    val newAncestors = ancestors.slice(newestToAdd, ancestors.size)
                    if (newAncestors.size > 7) {

                      val toAdd = newAncestors.slice(0, newAncestors.size - 5)

                      //val toAdd = ancestors.filterNot(last1000BundleHashes.contains)
                      if (toAdd.nonEmpty) {
                        activeDAGBundles = Seq()
                        println("Adding from ancestor extraction")
                        totalNumValidBundles += toAdd.size
                        if (last1000BundleHashes.size > 1000) {
                          last1000BundleHashes = (last1000BundleHashes ++ toAdd).slice(toAdd.size, 1000)
                        } else {
                          last1000BundleHashes ++= toAdd
                        }
                        val chainToAdd = toAdd.map {
                          bundleHashToBundle
                        }
                        val txToAdd = chainToAdd.flatMap(_.extractTX)
                        txToAdd.foreach {
                          t =>
                            acceptTransaction(t)
                        }
                        calculateReputationsFromScratch(totalNumValidBundles - toAdd.size - 1)
                        lastConfirmationUpdateTime = System.currentTimeMillis()
                      }
                    }


                  }

                }

        */

        //last1000BundleHashes.

      }


      val maxHeight = if (activeDAGBundles.isEmpty) 0 else activeDAGBundles.maxBy(_.meta.height).meta.height
      val minPBHash = if (totalNumValidBundles > 5 && activeDAGBundles.nonEmpty) {
        activeDAGBundles.groupBy(_.extractParentBundleHash).minBy(_._2.size)._1.pbHash
      } else ""

      activeDAGBundles = activeDAGBundles.filter{b =>
        !toRemove.contains(b) || b.meta.height < (maxHeight - 3) || b.extractParentBundleHash.pbHash == minPBHash
      }


      if (activeDAGBundles.size > 50) {
        activeDAGBundles = activeDAGBundles.sortBy(_.meta.totalScore).zipWithIndex.filter{_._2 < 30}.map{_._1}
      }


      /*
          bb.foreach { b =>
            val numPeersWithValid = peerSync.values.count{p =>
              p.bundle.contains{b} || p.validBundleHashes.contains(b.hash)
            }
            val peerFraction = numPeersWithValid.toDouble / peerSync.keys.size.toDouble

            if (System.currentTimeMillis() > (b.minTime + 20000) && peerFraction >= 0.5) {
              acceptBundle(b)
            }
          }

          val missingHashes = peerSync.flatMap{
            case (id, ps) =>
              ps.validBundleHashes
          }.toSet.filter{ z =>
            !validBundles.map{_.hash}.contains(z)
          }

          missingHashes.foreach{ z =>
            broadcast(RequestBundleData(z))
          }
      */

    }
  }

  def squashBundle(b: Bundle): Bundle = {
    logger.debug(s"Squashing bundle - ${b.pretty}")
    val squashed = Bundle(BundleData(Seq(b.bundleHash)).signed())
    validBundles :+= b
    bestBundleBase = b
    bestBundleCandidateHashes += b.bundleHash
    lastSquashed = Some(squashed)
    // broadcast(squashed)
    squashed
  }

  // def handleGossip(g: Gossip[ProductHash], remote: InetSocketAddress): Unit = {
  def handleGossip(gm : GossipMessage, remote: InetSocketAddress): Unit = {

    val rid = peerLookup.get(remote).map{_.data.id}

    gm match {

      case BatchBundleHashRequest(hashes) =>

        hashes.foreach{h =>
          db.getAs[BundleMetaData](h).foreach{
            b =>
              udpActor.udpSend(b.bundle, remote)
          }
        }

      case BatchTXHashRequest(hashes) =>

        hashes.foreach{h =>
          db.getAs[TX](h).foreach{
            b =>
              udpActor.udpSend(b, remote)
          }
        }

      case b: Bundle => handleBundle(b)

      case tx: TX =>

        if (!db.contains(tx)) {
          syncPendingTXHashes -= tx.hash
          db.put(tx)
          numSyncedTX += 1
        }

      case bb: PeerSyncHeartbeat =>
        handleBundle(bb.bestBundle)
        rid.foreach{ r =>
          peerSync(r) = bb
        }

      case g : Gossip[ProductHash] =>
      // handleGossipRegular(g, remote)
      case x =>
        logger.debug("Unrecognized gossip message " + x)
    }
  }



}
