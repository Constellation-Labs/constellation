package org.constellation.p2p

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Terminated}
import akka.http.scaladsl.model.StatusCodes
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.LevelDB
import org.constellation.consensus.Consensus.{PeerMemPoolUpdated, PeerProposedBlock}
import org.constellation.p2p.PeerToPeer._
import org.constellation.primitives.Schema.{Gossip, TX}
import org.constellation.primitives.Block
import org.constellation.state.MemPoolManager.AddTransaction
import org.constellation.util.{Heartbeat, ProductHash, Signed}

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Try}
import org.constellation.primitives.Schema._

object PeerToPeer {

  case class AddPeerFromLocal(address: InetSocketAddress)

  case class PeerRef(address: InetSocketAddress)

  case class Peers(peers: Seq[InetSocketAddress])

  case class Id(id: PublicKey) {
    def short: String = id.toString.slice(15, 20)
    def medium: String = id.toString.slice(15, 25).replaceAll(":", "")
    def address: Address = pubKeyToAddress(id)
  }

  case class GetPeers()


  case class GetId()

  case class GetBalance(account: PublicKey)

  case class HandShake(
                        originPeer: Signed[Peer],
                        requestExternalAddressCheck: Boolean = false
                        //           peers: Seq[Signed[Peer]],
                        //          destination: Option[InetSocketAddress] = None
                      ) extends ProductHash

  // These exist because type erasure messes up pattern matching on Signed[T] such that
  // you need a wrapper case class like this
  case class HandShakeMessage(handShake: Signed[HandShake])
  case class HandShakeResponseMessage(handShakeResponse: Signed[HandShakeResponse])

  case class HandShakeResponse(
                                //                   original: Signed[HandShake],
                                response: HandShake,
                                detectedRemote: InetSocketAddress
                              ) extends ProductHash

  case class SetExternalAddress(address: InetSocketAddress)

  case class GetExternalAddress()

  case class Peer(
                   id: Id,
                   externalAddress: InetSocketAddress,
                   remotes: Set[InetSocketAddress] = Set()
                 ) extends ProductHash

  case class Broadcast[T <: AnyRef](data: T)

  case class DBQuery(key: String)

}

class PeerToPeer(
                  publicKey: PublicKey,
                  system: ActorSystem,
                  consensusActor: ActorRef,
                  val udpActor: ActorRef,
                  val selfAddress: InetSocketAddress = new InetSocketAddress("127.0.0.1", 16180),
                  keyPair: KeyPair = null,
                  chainStateActor : ActorRef = null,
                  memPoolActor : ActorRef = null,
                  var requestExternalAddressCheck: Boolean = false,
                  val heartbeatEnabled: Boolean = false,
                  db: LevelDB = null
                )
                (implicit timeoutI: Timeout) extends Actor with ActorLogging with PeerAuth with Heartbeat {

  val id = Id(publicKey)
  implicit val timeout: Timeout = timeoutI
  implicit val kp: KeyPair = keyPair

  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher
  implicit val actorSystem: ActorSystem = context.system

  val logger = Logger(s"PeerToPeer")

  @volatile var memPoolTX: Set[TX] = Set()

  @volatile var validTX: Set[TX] = Set()

  @volatile var validSyncPendingTX: Set[TX] = Set()

 // @volatile var bundlePool: Set[Bundle] = Set()

 // @volatile var superSelfBundle : Option[Bundle] = None

  private val validUTXO = mutable.HashMap[String, Long]()

  private val validSyncPendingUTXO = mutable.HashMap[String, Long]()

  private val memPoolUTXO = mutable.HashMap[String, Long]()

  // TODO: Make this a graph to prevent duplicate storages.
  private val txToGossipChains = mutable.HashMap[String, Seq[Gossip[ProductHash]]]()

  // This should be identical to levelDB hashes but I'm putting here as a way to double check
  // Ideally the hash workload should prioritize memory and dump to disk later but can be revisited.
  private val addressToTX = mutable.HashMap[String, TX]()

  def selfBalance: Option[Long] = validUTXO.get(id.address.address)

  def acceptTransaction(tx: TX, updatePending: Boolean = true): Unit = {
    validTX += tx
    memPoolTX -= tx
    if (updatePending) {
      tx.updateUTXO(validSyncPendingUTXO)
    }
    validSyncPendingTX -= tx
    tx.updateUTXO(validUTXO)
    if (tx.tx.data.isGenesis) {
      tx.updateUTXO(memPoolUTXO)
    }
//    txToGossipChains.remove(tx.hash) // TODO: Remove after a certain period of time instead. Cleanup gossip data.
    val txSeconds = (System.currentTimeMillis() - tx.tx.time) / 1000
  //  logger.debug(s"Accepted TX from $txSeconds seconds ago: ${tx.short} on ${id.short} " +
   //   s"new mempool size: ${memPoolTX.size} valid: ${validTX.size} isGenesis: ${tx.tx.data.isGenesis}")
  }

  @volatile var downloadMode: Boolean = true

  @volatile var totalNumGossipMessages = 0

  var lastHeartbeatTime: Long = System.currentTimeMillis()

  // @volatile var downloadResponses = Seq[DownloadResponse]()
  var secretReputation: Map[Id, Double] = Map()
  var publicReputation: Map[Id, Double] = Map()
  var deterministicReputation: Map[Id, Double] = Map()

  def updateGossipChains(txHash: String, g: Gossip[ProductHash]): Unit = {
    val gossipSeq = g.iter
    val chains = txToGossipChains.get(txHash)
    if (chains.isEmpty) txToGossipChains(txHash) = Seq(g)
    else {
      val chainsA = chains.get
      val updatedGossipChains = chainsA.filter { c =>
        !c.iter.map {_.hash}.zip(gossipSeq.map {_.hash}).forall { case (x, y) => x == y }
      } :+ g
      txToGossipChains(txHash) = updatedGossipChains
    }
  }

  override def receive: Receive = {

    // This is unsafe due to type erasure, fix later.
    case ur: Seq[UpdateReputation] =>
  //    logger.debug(s"Reputation updates: $ur")
      secretReputation = ur.flatMap{ r => r.secretReputation.map{id -> _}}.toMap
      publicReputation = ur.flatMap{ r => r.publicReputation.map{id -> _}}.toMap

    case InternalHeartbeat =>

      processHeartbeat {
        if (downloadMode && peers.nonEmpty) {
          logger.debug("Requesting data download")
          broadcast(DownloadRequest())
        }

        if (!downloadMode) {
    //      broadcast(SyncData(validTX, memPoolTX))
        }

        val numAccepted = this.synchronized {
          val gs = txToGossipChains.values.map { g =>
            val tx = g.head.iter.head.data.asInstanceOf[TX]
            tx -> g
          }.toSeq.filter{z => !validTX.contains(z._1)}

          val filtered = gs.filter { case (tx, g) =>
            val lastTime = g.map {
              _.iter.last.time
            }.max
            val sufficientTimePassed = lastTime < (System.currentTimeMillis() - 5000)
            sufficientTimePassed
          }

          val acceptedTXs = filtered.map {_._1}

          acceptedTXs.foreach { z => acceptTransaction(z)}

          if (acceptedTXs.nonEmpty) {
            // logger.debug(s"Accepted transactions on ${id.short}: ${acceptedTXs.map{_.short}}")
          }

          // TODO: Add debug information to log metrics like number of peers / messages total etc.
          // logger.debug(s"P2P Heartbeat on ${id.short} - numPeers: ${peers.length}")

          // Send heartbeat here to other peers.
          acceptedTXs.size
        }

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

        logger.debug(
          s"Heartbeat: ${id.short}, memPool: ${memPoolTX.size} numPeers: ${peers.size} gossip: $totalNumGossipMessages, balance: $selfBalance, " +
            s"numAccepted: $numAccepted, numTotalValid: ${validTX.size} " +
            s"validUTXO: ${validUTXO.map { case (k, v) => k.slice(0, 5) -> v }} " +
            s"peers: ${peers.map { p => p.data.id.short + "-" + p.data.externalAddress + "-" + p.data.remotes }.mkString(",")}"
        )
    }

    case UDPMessage(d: DownloadResponse, remote) =>

      if (d.validTX.nonEmpty && d.validUTXO.nonEmpty) {
        validTX = d.validTX
        d.validUTXO.foreach{ case (k,v) =>
          validUTXO(k) = v
          memPoolUTXO(k) = v
        }
        downloadMode = false
        logger.debug("Downloaded data")
      }


    case UDPMessage(d: RequestTXProof, remote) =>
      val t = d.txHash
      val gossipChains = txToGossipChains.getOrElse(t, Seq())
      if (gossipChains.nonEmpty) {
        val t = gossipChains.head.iter.head.data.asInstanceOf[TX]
        udpActor.udpSend(MissingTXProof(t, gossipChains), remote)
      }

    case UDPMessage(d: MissingTXProof, remote) =>

      val t = d.tx

      if (!validSyncPendingTX.contains(t)) {
        if (t.utxoValid(validSyncPendingUTXO)) {
          t.updateUTXO(validSyncPendingUTXO)
        } else {
          // Handle double spend conflict here later, for now just drop
        }
      }

      d.gossip.foreach{
        g =>
          updateGossipChains(d.tx.hash, g)
      }

    case UDPMessage(d: SyncData, remote) =>

      val rid = peerLookup(remote)
      val diff = d.validTX.diff(validTX)
      if (diff.nonEmpty) {
        logger.debug(s"Desynchronization detected between remote: ${rid.short} and self: ${id.short} - diff size : ${diff.size}")
      }

      val selfMissing = d.validTX.filter{!validTX.contains(_)}

      selfMissing.foreach{ t =>
        if (!validSyncPendingTX.contains(t)) {
          if (t.utxoValid(validSyncPendingUTXO)) {
            t.updateUTXO(validSyncPendingUTXO)
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

    case UDPMessage(d: DownloadRequest, remote) =>

      val downloadResponse = DownloadResponse(validTX, validUTXO.toMap)
      udpActor.udpSend(downloadResponse, remote)

    case GetValidTX => sender() ! validTX

    case GetUTXO =>
      sender() ! validUTXO.toMap

    case GetMemPoolUTXO =>
      sender() ! memPoolUTXO.toMap

    case DBQuery(key) =>

      sender() ! db.get(key)

    case tx: TX =>

      this.synchronized {
        //if (tx.utxoValid(validUTXO) && tx.utxoValid(memPoolUTXO) && !memPoolTX.contains(tx)) {
        if (!memPoolTX.contains(tx) && !tx.tx.data.isGenesis) {
          tx.updateUTXO(memPoolUTXO)
          memPoolTX += tx
        }

        if (tx.tx.data.isGenesis) {
          acceptTransaction(tx)
          // We started genesis
          downloadMode = false
        }

        val g = Gossip(tx.signed())
        broadcast(g)
      }

    case UDPMessage(cd: ConflictDetected, _) =>

      // TODO: Verify it's an actual conflict relative to our current validation state
      // by reapplying the balances.
      memPoolTX -= cd.conflict.data.detectedOn
      cd.conflict.data.conflicts.foreach{ c =>
        memPoolTX -= c
      }

    case UDPMessage(g @ Gossip(e), remote) =>

      totalNumGossipMessages += 1
      this.synchronized {

        val gossipSeq = g.iter
        val tx = gossipSeq.head.data.asInstanceOf[TX]

        if (tx.tx.data.isGenesis) {
          acceptTransaction(tx)
        } else if (!downloadMode) {

          if (validTX.contains(tx)) {
         //   logger.debug(s"Ignoring gossip on already validated transaction: ${tx.short}")
          } else {

            if (!tx.utxoValid(validUTXO)) {
              // TODO: Add info explaining why transaction was invalid. I.e. InsufficientBalanceException etc.
              logger.debug(s"Ignoring invalid transaction ${tx.short} detected from p2p gossip")
            } else if (!tx.utxoValid(memPoolUTXO)) {
              logger.debug(s"Conflicting transactions detected ${tx.short}")
              // Find conflicting transactions
              val conflicts = memPoolTX.toSeq.filter { m =>
                tx.tx.data.src.map {
                  _.address
                }.intersect(m.tx.data.src.map {
                  _.address
                }).nonEmpty
              }
              val chains = conflicts.map { c =>
                VoteCandidate(c, txToGossipChains.getOrElse(c.hash, Seq()))
              }
              val newTXVote = VoteCandidate(tx, txToGossipChains.getOrElse(tx.hash, Seq()))
              val chooseOld = chains.map {
                _.gossip.size
              }.sum > newTXVote.gossip.size
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
                tx.updateUTXO(memPoolUTXO)
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

    case Broadcast(data) => broadcast(data.asInstanceOf[AnyRef])

    case UDPMessage(b: Block, _) =>
      chainStateActor ! b

    case a @ AddTransaction(transaction) =>
      logger.debug(s"Broadcasting TX ${transaction.short} on ${id.short}")
      broadcast(a)

    case UDPMessage(t: AddTransaction, remote) =>
      memPoolActor ! t

    case GetExternalAddress() => sender() ! externalAddress

    case SetExternalAddress(addr) =>
      logger.debug(s"Setting external address to $addr from RPC request")
      externalAddress = addr

    case AddPeerFromLocal(peerAddress) =>
      logger.debug(s"AddPeerFromLocal inet: ${pprintInet(peerAddress)}")

      this.synchronized {
        peerLookup.get(peerAddress) match {
          case Some(peer) =>
            logger.debug(s"Disregarding request, already familiar with peer on $peerAddress - $peer")
            sender() ! StatusCodes.AlreadyReported
          case None =>
            logger.debug(s"Peer $peerAddress unrecognized, adding peer")
            val attempt = Try {
              initiatePeerHandshake(PeerRef(peerAddress))
            }
            attempt match {
              case Failure(e) => e.printStackTrace(
              )
              case _ =>
            }

            val code = attempt.getOrElse(StatusCodes.InternalServerError)
            sender() ! code
        }
      }

    case GetPeers => sender() ! Peers(peerIPs.toSeq)

    case GetPeersID => sender() ! peers.map{_.data.id}
    case GetPeersData => sender() ! peers.map{_.data}

    case UDPSendToID(dataA, remoteId) =>
      peerIDLookup.get(remoteId).foreach{
        r =>
          //    logger.debug(s"UDPSendFOUND to ID on consensus : $data $remoteId")
          udpActor ! UDPSendTyped(dataA, r.data.externalAddress)
      }

    // Add type bounds here on all the command forwarding types
    // I.e. PeerMemPoolUpdated extends ConsensusCommand
    // Just check on ConsensusCommand and send to consensus actor automatically
    case UDPMessage(p: PeerMemPoolUpdated, remote) =>
      //  logger.debug("UDP PeerMemPoolUpdated received")
      consensusActor ! p

    case UDPMessage(p : PeerProposedBlock, remote) =>
      consensusActor ! p

    case UDPMessage(sh: HandShakeResponseMessage, remote) =>
      handleHandShakeResponse(sh, remote)

    case UDPMessage(sh: HandShakeMessage, remote) =>
      handleHandShake(sh, remote)

    case UDPMessage(peersI: Peers, remote) =>
      peersI.peers.foreach{
        p =>
          self ! PeerRef(p)
      }

    case UDPMessage(_: Terminated, remote) =>
      logger.debug(s"Peer $remote has terminated. Removing it from the list.")
    // TODO: FIX
    // peerIPs -= remote

    case u: UDPMessage =>
      logger.error(s"Unrecognized UDP message: $u")
  }

}

