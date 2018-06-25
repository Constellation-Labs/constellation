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
      bundleHeartbeat()
    }
    0

    /*
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
    */
  }

  def updateMempool(tx: TX): Boolean = {
    val validUpdate = !memPoolTX.contains(tx) && !tx.tx.data.isGenesis && tx.valid && tx.ledgerValid(memPoolLedger) &&
      !validTX.contains(tx)
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

    /*
        if (!tx.tx.data.isGenesis) {
          // val unspokenTX = memPoolTX
          val b = Bundle(BundleData(Seq(
            tx,
            lastBundleHash
          )).signed())
          processNewBundleMetadata(b)
          broadcast(b)
        }
    */

  }

  def acceptBundle(b: Bundle): Unit = {
    validBundles :+= b
    calculateReputationsFromScratch()
    b.extractTX.foreach{ t =>
      acceptTransaction(t)
    }
    activeDAGBundles = Seq()
  }

  def genesisCheck(): Unit = {


    val bbExist = Option(bestBundle).exists {_.txBelow.size >= 2}

    //  logger.debug(s"Genesis check $genesisAdditionCheck $bbExist")

    if ( genesisAdditionCheck &&
      bbExist
    ) {
      // squashBundle(bestBundle)
      acceptBundle(bestBundle)
    }
/*
    if (lastBundleHash == genesisBundle.bundleHash && !(genesisBundle.extractIds.head == id)) {
      peerSync.get(genesisBundle.extractIds.head).foreach{ b =>
        if (b.validBundleHashes.last != genesisBundle.hash && b.lastBestBundle.extractTX.size >= 2) {
          acceptBundle(b.lastBestBundle)
        }
      }
    }
    */

  }

  def genesisAdditionCheck: Boolean =
    lastBundleHash.hash == genesisBundle.hash && // This is the first bundle creation attempt after genesis
      genesisBundle.extractIds.head == id // This node created the genesis bundle

  def bundleHeartbeat(): Unit = {

    activeDAGBundles = activeDAGBundles.sortBy(_.meta.totalScore)

    val bb: Option[Bundle] = activeDAGBundles.headOption

    bb.foreach{bestBundle = _}

    if (!downloadMode) {
      broadcast(PeerSyncHeartbeat(bb, memPoolTX, validBundles.map{_.hash}))

      // || peers have no bundles / stalled.
      val memPoolEmit = Random.nextInt() < 0.3 // && (System.currentTimeMillis() < lastBundle.maxTime + 25000)

      if (memPoolTX.nonEmpty && (memPoolEmit || genesisAdditionCheck)) {
        // Emit an origin bundle. This needs to be managed by prob facil check on hash of previous + ids
        val memPoolSelSize = Random.nextInt(45)
        val memPoolSelection = Random.shuffle(memPoolTX.toSeq).slice(0, memPoolSelSize + 3)
        val b = Bundle(BundleData(
          memPoolSelection :+ bb.map{z => ParentBundleHash(z.hash)}.getOrElse(lastBundleHash)
        ).signed())
        processNewBundleMetadata(b, memPoolSelection.toSet)
        broadcast(b)
      }
    }

    genesisCheck()

    //  if (System.currentTimeMillis() < (lastBundle.maxTime + 10000)) {
    val nonSelfIdCandidates = activeDAGBundles.filter { b => !b.idBelow.contains(id) }
    logger.debug(s"Num nonSelfIdCandidates ${id.short} ${nonSelfIdCandidates.size}")

    nonSelfIdCandidates
      .groupBy(b => b.maxStackDepth -> b.extractParentBundleHash).foreach {
      case (_, bundles) =>
        //val sorted = bundles.sortBy(b => (b.idBelow.size, b.txBelow.size, b.hash))
        val random = Random.shuffle(bundles).slice(0, 5)
        val iterator = random.combinations(2)
        iterator.foreach {
          case both@Seq(l, r) =>
            val hasNewTransactions = l.txBelow.diff(r.txBelow).nonEmpty
            val minTimeClose = Math.abs(l.minTime - r.minTime) < 25000
            if (hasNewTransactions && minTimeClose && Random.nextDouble() > 0.5) {
              val b = Bundle(BundleData(both).signed())
              processNewBundleMetadata(b, b.extractTX)
              broadcast(b)
              println(s"Created new depth bundle ${b.maxStackDepth}")
            }
        }
    }



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

  def validateTXBatch(txs: Set[TX]): Boolean =
    validateTransactionBatch(txs, memPoolLedger) &&
      validateTransactionBatch(txs, validLedger)

  def handleBundle(bundle: Bundle): Unit = {

    totalNumBundleMessages += 1

    val notPresent = !bundleHashToBundle.contains(bundle.hash)

    if (notPresent) {
      val parentHash = bundle.extractParentBundleHash.hash
      val parentKnown = bundleHashToBundle.contains(parentHash)
      if (!parentKnown) {
        totalNumBundleHashRequests += 1
        broadcast(RequestBundleData(parentHash))
      } else {
        val txs = bundle.extractTX
        // TODO: Need to register potentially invalid bundles somewhere but not use them in case of fork download.
        val validByParent = validateTXBatch(txs)
        if (validByParent) {
          totalNumNewBundleAdditions += 1
          txs.foreach{ t =>
            if (!t.tx.data.isGenesis) memPoolTX += t
          }
          processNewBundleMetadata(bundle, txs)
        } else {
          totalNumInvalidBundles += 1
        }
      }
    }

    // Also need to verify there are not multiple occurrences of same id re-signing bundle, and reps.
  //  val valid = validateTXBatch(txs) && txs.intersect(validTX).isEmpty

  }

  // def handleGossip(g: Gossip[ProductHash], remote: InetSocketAddress): Unit = {
  def handleGossip(gm : GossipMessage, remote: InetSocketAddress): Unit = {

    val rid = peerLookup(remote).data.id

    gm match {
      case g : Gossip[ProductHash] =>
      // handleGossipRegular(g, remote)
      case bb: PeerSyncHeartbeat =>
        //    println(s"RECEIVED PEER SYNC OF MEMPOOL SIZE ${bb.memPool.size}")
        bb.bundle.foreach{b => handleBundle(b)}
        peerSync(rid) = bb
        if (validateTXBatch(bb.memPool)) {
          bb.memPool.foreach{updateMempool}
        }
      case b: Bundle =>
        handleBundle(b)
      case sd: SyncData =>
      //      handleSyncData(sd, remote)
      case RequestBundleData(hash) =>
        bundleHashToBundle.get(hash).foreach{ b =>
          udpActor.udpSendToId(b, rid)
        }
      case _ =>
        logger.debug("Unrecognized gossip message")
    }
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


}
