package org.constellation.p2p

import java.net.InetSocketAddress

import org.constellation.Data
import org.constellation.primitives.Schema._
import org.constellation.util.ProductHash
import constellation._

import scala.collection.concurrent.TrieMap
import scala.util.{Random, Try}

trait ProbabilisticGossip extends PeerAuth {

  val data: Data

  import data._

  def updateGossipChains(txHash: String, g: Gossip[ProductHash]): Unit = {
    val gossipSeq = g.iter
    val chains = txToGossipChains.get(txHash)
    if (chains.isEmpty) txToGossipChains(txHash) = Seq(g)
    else {
      val chainsA = chains.get
      val updatedGossipChains = chainsA.filter { c =>
        !c.iter.map {
          _.hash
        }.zip(gossipSeq.map {
          _.hash
        }).forall { case (x, y) => x == y }
      } :+ g
      txToGossipChains(txHash) = updatedGossipChains
    }
  }

  def gossipHeartbeat(): Int = {

    if (!downloadMode) {
      //  broadcast(SyncData(validTX, memPoolTX))


      val numAccepted = this.synchronized {
        val gs = txToGossipChains.values.map { g =>
          val tx = g.head.iter.head.data.asInstanceOf[TX]
          tx -> g
        }.toSeq.filter { z => !validTX.contains(z._1) }

        val filtered = gs.filter { case (tx, g) =>
          val lastTime = g.map {
            _.iter.last.time
          }.max
          val sufficientTimePassed = lastTime < (System.currentTimeMillis() - 5000)
          sufficientTimePassed
        }

        val acceptedTXs = filtered.map {
          _._1
        }

        acceptedTXs.foreach { z => acceptTransaction(z) }

        if (acceptedTXs.nonEmpty) {
          // logger.debug(s"Accepted transactions on ${id.short}: ${acceptedTXs.map{_.short}}")
        }

        // TODO: Add debug information to log metrics like number of peers / messages total etc.
        // logger.debug(s"P2P Heartbeat on ${id.short} - numPeers: ${peers.length}")

        // Send heartbeat here to other peers.
        acceptedTXs.size
      }
      /*

          validSyncPendingTX.foreach{
            tx =>
              val chains = txToGossipChains.get(tx.hash)
              chains.foreach{
                c =>
                  val lastTime = c.map {_.iter.last.time}.max
                  val sufficientTimePassed = lastTime < (System.currentTimeMillis() - 5000)
                  sufficientTimePassed
              }
          }
      */

      bundleHeartbeat()


      numAccepted

    } else 0
  }

  def updateMempool(tx: TX): Boolean = {
    val validUpdate = !memPoolTX.contains(tx) && !tx.tx.data.isGenesis && tx.valid && tx.ledgerValid(memPoolLedger)
    if (validUpdate) {
      tx.updateLedger(memPoolLedger)
      memPoolTX += tx
    }
    validUpdate
  }

  def handleLocalTransactionAdd(tx: TX): Unit = {

    if (!txHashToTX.contains(tx.hash)) txHashToTX(tx.hash) = tx

    // TODO: Assume TX has already been validated during API request.
    updateMempool(tx)

    // TODO: Fix this
    if (tx.tx.data.isGenesis) {
      createGenesis(tx)
      acceptTransaction(tx)
      // We started genesis
      downloadMode = false
    }

    // Temp
   // val g = Gossip(tx.signed())
   // broadcast(g)

    //val unspokenTX = memPoolTX.filter{!txsGossipedAbout.contains(_)}
    //  txsGossipedAbout ++= unspokenTX

    if (!tx.tx.data.isGenesis) {
      // val unspokenTX = memPoolTX
      val b = Bundle(BundleData(Seq(
        tx,
        lastBundleHash
      )).signed())
      processNewBundleMetadata(b)
      broadcast(b)
    }

  }

  def acceptBundle(b: Bundle): Unit = {
    validBundles :+= b
    b.extractTX.foreach{ t =>
      acceptTransaction(t)
    }
    activeBundles = Seq()
  }

  def bundleHeartbeat(): Unit = {

    val bb: Option[Bundle] = if (activeBundles.isEmpty) None else {
      Some(activeBundles.maxBy{ b => (b.extractIds.size, b.extractTX.size, b.hash)})
    }

    bb.foreach{bestBundle = _}

    if (!downloadMode) {
      broadcast(BestBundle(bb, lastBundle, lastSquashed))
    }


    if (
      lastBundleHash == genesisBundle.bundleHash && // This is the first bundle creation attempt after genesis
        genesisBundle.extractIds.head == id && // This node created the genesis bundle
        Option(bestBundle).exists{_.extractIds.size >= 2} // Bundle is sufficient to package
    ) {
      // squashBundle(bestBundle)
      acceptBundle(bestBundle)
    }

    if (lastBundleHash == genesisBundle.bundleHash && !(genesisBundle.extractIds.head == id)) {
      bestBundles.get(genesisBundle.extractIds.head).foreach{ b =>
        b.bundle.foreach{ ba =>
          if (ba.extractIds.size >= 2) {
            acceptBundle(ba)
          }
        }
      }
    }



    /*

        val bb = if (bundles.nonEmpty) {

          val bestBundle = bundles.toSeq
            .groupBy(_.extractTX.size).maxBy(_._1)._2
            .groupBy(_.maxStackDepth).maxBy {
            _._1
          }._2
            .groupBy(_.extractTX.toSeq.map {
              _.hash
            }.sorted.mkString).maxBy(_._1)._2
            .maxBy(_.hash)

          logger.debug(s"best bundle on ${id.short} ${bestBundle.short} ${
            bestBundle.extractTX.map {
              _.short
            }
          }" +
            s" hashes: ${
              bundles.toSeq.map { z: Bundle => z.maxStackDepth -> z.extractTX.map {
                _.short
              }
              }
            }")
          Some(bestBundle)
        }
        else None

        broadcast(BestBundle(bb, bestBundleSelf))
    */

    /*
        if (bestBundles.nonEmpty) {

          val b2Id = {
            bestBundles.toSeq.map { case (peerId, b) => b -> peerId } ++ Seq(bestBundle -> id)
          }.groupBy(_._1)

          val best = b2Id.map {
            case (b, ids) =>
              b.short -> ids.length
          }

          logger.debug(s"peer bundles on ${id.short} mine ${bestBundle.short} - $best")

          if (b2Id.keys.toSeq.map {
            _.extractTX
          }.toSet.size == 1 && bestBundles.keys.size == peers.size) {
            // all TX the same in all bundles received
            val bundle = b2Id.maxBy(_._2.size)._1
            bestBundleSelf = bundle
            bundles -= bundle
          }


        }
    */

  }


  /* val oldBundles = bundles.filter{ bi =>
     bi.bundleData.time < (System.currentTimeMillis() - 25000)
   }
   bundles --= oldBundles

   if (bundles.nonEmpty) {
     val b = Bundle(BundleData(bundles.toSeq).signed())
     broadcast(b)
   }
   */


  //val results = searchLeft.flatMap{ l =>
  //searchRight.flatMap{ r =>

  //  val searchLeft = bundles.slice(0, 10)
  //  val searchRight = bundles.slice(10, bundles.length)


  def findCommonSubBundles() = {

    val results = activeBundles.combinations(2).toSeq.flatMap{
      case Seq(l,r) =>
        val sub = l.extractSubBundlesMinSize()
        val subr = r.extractSubBundlesMinSize()
        sub.intersect(subr).toSeq.map{
          b => b -> 1
        }
    }.groupBy(_._1).map{
      case (x,y) => x -> y.size
    }

    val debug = results.map{case (x,y) => (x.short, x.extractTX.size, y)}.toSeq.sortBy{_._2}.reverse.slice(0, 10)
    println(s"bundle common ${id.short} : ${results.size} $debug")
    results
  }

  def validateTransactionBatch(txs: Set[TX], ledger: TrieMap[String, Long]): Boolean = {
    txs.toSeq.map{ tx =>
      tx.tx.data.src.head.address -> tx.tx.data.amount
    }.groupBy(_._1).forall{
      case (a, seq) =>
        val bal = ledger.getOrElse(a, 0L)
        bal >= seq.map{_._2}.sum
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

  def handleBundle(bundle: Bundle): Unit = {

    totalNumBundleMessages += 1

    val txs = bundle.extractTX

    // Also need to verify there are not multiple occurrences of same id re-signing bundle, and reps.
    val valid = validateTransactionBatch(txs, memPoolLedger) && validateTransactionBatch(txs, validLedger)
    val hasCorrectBaseHash = bundle.extractBundleHash == lastBundleHash
    val hasNewTransactions = txs.exists{ t => !memPoolTX.contains(t)}

    if (valid) {

      val neverSeen = processNewBundleMetadata(bundle)

      if (!hasCorrectBaseHash) {
        bestBundleCandidateHashes += bundle.extractBundleHash
      }

      if (neverSeen) {

        txs.foreach(updateMempool)
        val ids = bundle.extractIds

        // This bundle doesn't contain an observation from us, (apart from any squashed in a BundleHash)
        // Hence we should process it.

        if (!ids.contains(id)) {

          val sortedBundles = activeBundles.filter{ b => Try{b.bundleScore}.isSuccess}.sortBy { b => -1 * b.bundleScore }

          // Find similar bundles to merge this with to span the maximal set of ids of similar stack depth.

          val commonSubBundles = findCommonSubBundles()

          if (activeBundles.length > 30) {
            if (Random.nextDouble() < 0.4) {
              val remove = Random.shuffle(activeBundles.filter { b => !commonSubBundles.contains(b) }).slice(0, 10)
              activeBundles = activeBundles.filterNot(remove.contains)
              if (commonSubBundles.size > 1) {
                commonSubBundles.toSeq.sortBy(_._2)
              }
            }
          }

          var newTXs = txs
          val filtered = (activeBundles ++ commonSubBundles.keys).filter { bi =>
            val theseTX = bi.extractTX
            val timeValid = bi.bundleData.time > (bundle.bundleData.time - 25000)
            val res = theseTX.exists(!newTXs.contains(_)) && timeValid
            if (res) newTXs ++= theseTX
            res
          }
          val fIds = filtered.flatMap {
            _.extractIds
          }.toSet ++ ids
          val emitter = filtered :+ bundle

          if (hasNewTransactions) {
            val b = Bundle(BundleData(emitter).signed())
            if (!bundleHashToBundle.contains(b.hash)) {
              processNewBundleMetadata(b)
              broadcast(b, skipIDs = fIds.toSeq)
            }
          }
          // bundles = bundles.sorted
        }
      }

    }
  }

  // def handleGossip(g: Gossip[ProductHash], remote: InetSocketAddress): Unit = {
  def handleGossip(gm : GossipMessage, remote: InetSocketAddress): Unit = {

    val rid = peerLookup(remote).data.id

    gm match {
      case g : Gossip[ProductHash] =>
        // handleGossipRegular(g, remote)
      case bb: BestBundle =>
        bb.bundle.foreach{b => processNewBundleMetadata(b)}
        Option(bb.lastBestBundle).foreach{b => processNewBundleMetadata(b)}
        bestBundles(rid) = bb
      case b: Bundle =>
        handleBundle(b)
      case sd: SyncData =>
      //      handleSyncData(sd, remote)
      case _ =>
        logger.debug("Unrecognized gossip message")
    }
  }


  def handleSyncData(d: SyncData, remote: InetSocketAddress): Unit = {

    val rid = peerLookup(remote)
    val diff = d.validTX.diff(validTX)
    if (diff.nonEmpty) {
      logger.debug(s"Desynchronization detected between " +
        s"remote: ${rid.short} and self: ${id.short} - diff size : ${diff.size}")
    }

    val selfMissing = d.validTX.filter{!validTX.contains(_)}

    selfMissing.foreach{ t =>
      if (!validSyncPendingTX.contains(t)) {
        if (t.ledgerValid(validSyncPendingUTXO)) {
          t.updateLedger(validSyncPendingUTXO)
        } else {
          // Handle double spend conflict here later, for now just drop
        }
      }
      validSyncPendingTX += t
      broadcast(RequestTXProof(t.hash))
    }


    val otherMissing = validTX.filter{!d.validTX.contains(_)}
    otherMissing.toSeq.foreach{ t =>
      val gossipChains = txToGossipChains.getOrElse(t.hash, Seq())
      if (gossipChains.nonEmpty) {
        udpActor.udpSend(MissingTXProof(t, gossipChains), remote)
      }
    }

    // logger.debug(s"SyncData message size: ${d.validTX.size} on ${validTX.size} ${id.short}")

  }


  def handleGossipRegular(g: Gossip[ProductHash], remote: InetSocketAddress): Unit = {

    totalNumGossipMessages += 1

    val gossipSeq = g.iter
    val tx = gossipSeq.head.data.asInstanceOf[TX]
    if (!txHashToTX.contains(tx.hash)) txHashToTX(tx.hash) = tx

    if (!downloadMode) {

      if (validTX.contains(tx)) {
        //   logger.debug(s"Ignoring gossip on already validated transaction: ${tx.short}")
      } else {

        if (!tx.ledgerValid(validLedger)) {
          // TODO: Add info explaining why transaction was invalid. I.e. InsufficientBalanceException etc.
          logger.debug(s"Ignoring invalid transaction ${tx.short} detected from p2p gossip")
        } else if (!tx.ledgerValid(memPoolLedger)) {
          logger.debug(s"Conflicting transactions detected ${tx.short}")
          // Find conflicting transactions
          val conflicts = memPoolTX.toSeq.filter { m =>
            tx.tx.data.src.map {_.address}.intersect(m.tx.data.src.map {_.address}).nonEmpty
          }

          val chains = conflicts.map { c =>
            VoteCandidate(c, txToGossipChains.getOrElse(c.hash, Seq()))
          }
          val newTXVote = VoteCandidate(tx, txToGossipChains.getOrElse(tx.hash, Seq()))
          val chooseOld = chains.map {_.gossip.size}.sum > newTXVote.gossip.size
          val accept = if (chooseOld) chains else Seq(newTXVote)
          val reject = if (!chooseOld) chains else Seq(newTXVote)

          memPoolTX -= tx
          conflicts.foreach { c =>
            memPoolTX -= c
          }

          // broadcast(Vote(VoteData(accept, reject).signed()))
          broadcast(ConflictDetected(ConflictDetectedData(tx, conflicts).signed()))
        } else {

          if (!memPoolTX.contains(tx)) {
            tx.updateLedger(memPoolLedger)
            memPoolTX += tx
            //    logger.debug(s"Adding TX to mempool - new size ${memPoolTX.size}")
          }

          updateGossipChains(tx.hash, g)

          // Continue gossip.
          val peer = peerLookup(remote).data.id
          val gossipKeys = gossipSeq.tail.flatMap{_.encodedPublicKeys}.distinct.map{_.toPublicKey}.map{Id}

          val containsSelf = gossipKeys.contains(id)
          val underMaxDepth = g.stackDepth < 6

          val transitionProbabilities = Seq(1.0, 1.0, 0.2, 0.1, 0.05, 0.001)
          val prob = transitionProbabilities(g.stackDepth - 1)
          val emit = scala.util.Random.nextDouble() < prob

          val skipIDs = (gossipKeys :+ peer).distinct
          val idsCanSendTo = peerIDLookup.keys.filter { k => !skipIDs.contains(k) }.toSeq

          val peerTransitionProbabilities = Seq(1.0, 1.0, 0.5, 0.3, 0.2, 0.1)
          val peerProb = peerTransitionProbabilities(g.stackDepth - 1)
          val numPeersToSendTo = (idsCanSendTo.size.toDouble * peerProb).toInt
          val shuffled = scala.util.Random.shuffle(idsCanSendTo)
          val peersToSendTo = shuffled.slice(0, numPeersToSendTo)

          ///     logger.debug(s"Gossip nodeId: ${id.medium}, tx: ${tx.short}, depth: ${g.stackDepth}, prob: $prob, emit: $emit, " +
          //      s"numPeersToSend: $numPeersToSendTo")

          if (underMaxDepth && !containsSelf && emit) {
            val gPrime = Gossip(g.signed())
            broadcast(gPrime, skipIDs = skipIDs, idSubset = peersToSendTo)
          }
        }
      }
    }

  }


  // var txsGossipedAbout = Set[TX]()

  // val txToPeersObserved

  /*

    case UDPMessage(d: RequestTXProof, remote) =>
      val t = d.txHash
      val gossipChains = txToGossipChains.getOrElse(t, Seq())
      if (gossipChains.nonEmpty) {
        val t = gossipChains.head.iter.head.data.asInstanceOf[TX]
        udpActor.udpSend(MissingTXProof(t, gossipChains), remote)
      }

    case UDPMessage(d: MissingTXProof, remote) =>

      val t = d.tx/*

      if (!validSyncPendingTX.contains(t)) {
        if (t.utxoValid(validSyncPendingUTXO)) {
          t.updateUTXO(validSyncPendingUTXO)
        } else {
          // Handle double spend conflict here later, for now just drop
        }
      }
*/
      d.gossip.foreach{
        g =>
          updateGossipChains(d.tx.hash, g)
      }

    case UDPMessage(d: SyncData, remote) =>

    case UDPMessage(b: Bundle, remote) =>

      val idsInBundle = b.extractIds
      val txsInBundle = b.extractTX

      if (bundles.nonEmpty) {
        BundleData((bundles.toSeq :+ b).distinct).signed()
      }

      // val newTXs = memPoolTX.filter{!txsInBundle.contains(_)}
      bundles += b


    case UDPMessage(cd: ConflictDetected, _) =>

      // TODO: Verify it's an actual conflict relative to our current validation state
      // by reapplying the balances.
      memPoolTX -= cd.conflict.data.detectedOn
      cd.conflict.data.conflicts.foreach{ c =>
        memPoolTX -= c
      }


   */

}
