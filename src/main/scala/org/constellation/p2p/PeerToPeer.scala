package org.constellation.p2p

import java.io.File
import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Terminated}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.serialization.SerializationExtension
import akka.util.{ByteString, Timeout}
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.consensus.Consensus.{PeerMemPoolUpdated, PeerProposedBlock, RequestBlockProposal}
import org.constellation.LevelDB
import org.constellation.consensus.Consensus.{Heartbeat, PeerMemPoolUpdated, PeerProposedBlock, RequestBlockProposal}
import org.constellation.p2p.PeerToPeer._
import org.constellation.primitives.Chain.Chain
import org.constellation.primitives.Schema.{AddressVertexCache, Gossip, TX}
import org.constellation.primitives.{Block, Transaction}
import org.constellation.state.MemPoolManager.AddTransaction
import org.constellation.util.{ProductHash, Signed}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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
                  udpActor: ActorRef,
                  selfAddress: InetSocketAddress = new InetSocketAddress("127.0.0.1", 16180),
                  keyPair: KeyPair = null,
                  chainStateActor : ActorRef = null,
                  memPoolActor : ActorRef = null,
                  @volatile var requestExternalAddressCheck: Boolean = false,
                  heartbeatEnabled: Boolean = false,
                  db: LevelDB = null
                )
                (implicit timeout: Timeout) extends Actor with ActorLogging {

  private val id = Id(publicKey)
  private implicit val kp: KeyPair = keyPair

  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher
  implicit val actorSystem: ActorSystem = context.system

  val logger = Logger(s"PeerToPeer")

  // @volatile private var peers: Set[InetSocketAddress] = Set.empty[InetSocketAddress]
  @volatile private var remotes: Set[InetSocketAddress] = Set.empty[InetSocketAddress]
  @volatile private var externalAddress: InetSocketAddress = selfAddress

  private val peerLookup = mutable.HashMap[InetSocketAddress, Signed[Peer]]()

  private def peerIDLookup = peerLookup.values.map{z => z.data.id -> z}.toMap

  private def selfPeer = Peer(id, externalAddress, Set()).signed()

  private def peerIPs = peerLookup.values.map(z => z.data.externalAddress).toSet

  private def allPeerIPs = {
    peerLookup.keys ++ peerLookup.values.flatMap(z => z.data.remotes ++ Seq(z.data.externalAddress))
  }.toSet

  private def peers = peerLookup.values.toSeq.distinct

  def broadcast[T <: AnyRef](message: T, skipIDs: Seq[Id] = Seq(), idSubset: Seq[Id] = Seq()): Unit = {
    val dest = if (idSubset.isEmpty) peerIDLookup.keys else idSubset
    dest.foreach{ i =>
      if (!skipIDs.contains(i)) self ! UDPSendToID(message, i)
    }
  }

  private def handShakeInner = {
    HandShake(selfPeer, requestExternalAddressCheck) //, peers)
  }

  def initiatePeerHandshake(p: PeerRef): StatusCode = {
    val peerAddress = p.address
    import akka.pattern.ask
    val banList = (udpActor ? GetBanList).mapTo[Seq[InetSocketAddress]].get()
    if (!banList.contains(peerAddress)) {
      val res = if (peerIPs.contains(peerAddress)) {
        logger.debug(s"We already know $peerAddress, discarding")
        StatusCodes.AlreadyReported
      } else if (peerAddress == externalAddress || remotes.contains(peerAddress)) {
        logger.debug(s"Peer is same as self $peerAddress, discarding")
        StatusCodes.BadRequest
      } else {
        logger.debug(s"Sending handshake from $externalAddress to $peerAddress with ${peers.size} known peers")
        //Introduce ourselves
        // val message = HandShakeMessage(handShakeInner.copy(destination = Some(peerAddress)).signed())
        val message = HandShakeMessage(handShakeInner.signed())
        udpActor.udpSend(message, peerAddress)
        //Tell our existing peers
        //broadcast(p)
        StatusCodes.Accepted
      }
      res
    } else {
      logger.debug(s"Attempted to add peer but peer was previously banned! $peerAddress")
      StatusCodes.Forbidden
    }
  }

  private def addPeer(
                       value: Signed[Peer],
                       newPeers: Seq[Signed[Peer]] = Seq()
                     ): Unit = {

    this.synchronized {
      peerLookup(value.data.externalAddress) = value
      value.data.remotes.foreach(peerLookup(_) = value)
      logger.debug(s"Peer added, total peers: ${peerIDLookup.keys.size} on $selfAddress")
      newPeers.foreach { np =>
        //    logger.debug(s"Attempting to add new peer from peer reference handshake response $np")
        //   initiatePeerHandshake(PeerRef(np.data.externalAddress))
      }
    }
  }

  private def banOn[T](valid: => Boolean, remote: InetSocketAddress)(t: => T) = {
    if (valid) t else {
      logger.debug(s"BANNING - Invalid HandShakeResponse from - $remote")
      udpActor ! Ban(remote)
    }
  }


  private val bufferTask = new Runnable { def run(): Unit = {

    // TODO: This may not be necessary when full metrics collector are set up,
    // Also this could be triggered by regular events instead of async.
    // Needs to be revisited if its required, certainly useful for debugging UDP connectivity though.
    Try {
      // TODO: Add debug information to log metrics like number of peers / messages total etc.
     // logger.debug(s"P2P Heartbeat on ${id.short} - numPeers: ${peers.length}")

      // Send heartbeat here to other peers.
    } match {
      case Failure(e) => e.printStackTrace()
      case _ =>
    }

  } }

  var heartBeatMonitor: ScheduledFuture[_] = _
  var heartBeat: ScheduledThreadPoolExecutor = _


  if (heartbeatEnabled) {
    heartBeat = new ScheduledThreadPoolExecutor(10)
    heartBeatMonitor = heartBeat.scheduleAtFixedRate(bufferTask, 1, 3, TimeUnit.SECONDS)
  }

  override def receive: Receive = {

    case DBQuery(key) =>

      sender() ! db.get(key)

    case tx: TX =>
      memPoolActor ! tx
      chainStateActor ! tx
      val g = Gossip(tx.signed())
      broadcast(g)

    case UDPMessage(g @ Gossip(e), remote) =>

      val gossipSeq = g.iter
      val tx = gossipSeq.head.data.asInstanceOf[TX]

      memPoolActor ! tx
      chainStateActor ! tx

      val peer = peerLookup(remote).data.id
      val gossipKeys = gossipSeq.flatMap{_.publicKeys}.distinct.map{Id}
      val containsSelf = gossipKeys.contains(id)
      val underMaxDepth = g.stackDepth < 6

      val transitionProbabilities = Seq(1.0, 0.5, 0.2, 0.1, 0.05, 0.001)
      val prob = transitionProbabilities(g.stackDepth - 1)
      val emit = scala.util.Random.nextDouble() < prob

      val skipIDs = (gossipKeys :+ peer).distinct
      val idsCanSendTo = peerIDLookup.keys.filter{k => !skipIDs.contains(k)}.toSeq

      val peerTransitionProbabilities = Seq(1.0, 0.8, 0.5, 0.3, 0.2, 0.1)
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

    case GetId =>
      sender() ! Id(publicKey)

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

    case UDPMessage(p : RequestBlockProposal, remote) =>

      println("RequestBlockProposal on " + id.short)

      import akka.pattern.ask

      chainStateActor ! p

    case UDPMessage(sh: HandShakeResponseMessage, remote) =>
      //    logger.debug(s"HandShakeResponseMessage from $remote on $externalAddress second remote: $remote")
      //  val o = sh.handShakeResponse.data.original
      //   val fromUs = o.valid && o.publicKeys.head == id.id
      // val valid = fromUs && sh.handShakeResponse.valid

      val address = sh.handShakeResponse.data.response.originPeer.data.externalAddress
      if (requestExternalAddressCheck) {
        externalAddress = sh.handShakeResponse.data.detectedRemote
        requestExternalAddressCheck = false
      }

      // ^ TODO : Fix validation
      banOn(sh.handShakeResponse.valid, remote) {
        logger.debug(s"Got valid HandShakeResponse from $remote / $address on $externalAddress")
        val value = sh.handShakeResponse.data.response.originPeer
        val newPeers = Seq() //sh.handShakeResponse.data.response.peers
        addPeer(value, newPeers)
        remotes += remote
      }

    case UDPMessage(sh: HandShakeMessage, remote) =>
      val hs = sh.handShake.data
      val address = hs.originPeer.data.externalAddress
      val responseAddr = if (hs.requestExternalAddressCheck) remote else address

      logger.debug(s"Got handshake from $remote on $externalAddress, sending response to $responseAddr")
      banOn(sh.handShake.valid, remote) {
        logger.debug(s"Got handshake inner from $remote on $externalAddress, " +
        s"sending response to $remote inet: ${pprintInet(remote)} " +
        s"peers externally reported address: ${hs.originPeer.data.externalAddress} inet: " +
        s"${pprintInet(address)}")
        val response = HandShakeResponseMessage(
          // HandShakeResponse(sh.handShake, handShakeInner.copy(destination = Some(remote)), remote).signed()
          HandShakeResponse(handShakeInner, remote).signed()
        )
        udpActor.udpSend(response, responseAddr)
        initiatePeerHandshake(PeerRef(responseAddr))
      }

    case UDPMessage(peersI: Peers, remote) =>
      peersI.peers.foreach{
        p =>
          self ! PeerRef(p)
      }
    case UDPMessage(h: Heartbeat, remote) =>
      logger.debug(s"Received heartbeat on ${id.short} from ${h.id.short} : $remote")

    case UDPMessage(_: Terminated, remote) =>
      logger.debug(s"Peer $remote has terminated. Removing it from the list.")
    // TODO: FIX
    // peerIPs -= remote

    case u: UDPMessage =>
      logger.error(s"Unrecognized UDP message: $u")
  }

}
