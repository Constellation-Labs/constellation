package org.constellation.p2p

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Terminated}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.util.Timeout
import constellation._
import org.constellation.consensus.Consensus.{PeerMemPoolUpdated, PeerProposedBlock}
import org.constellation.p2p.PeerToPeer._
import org.constellation.util.{ProductHash, Signed}

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor

object PeerToPeer {

  case class AddPeerFromLocal(address: InetSocketAddress)

  case class PeerRef(address: InetSocketAddress)

  case class Peers(peers: Seq[InetSocketAddress])

  case class Id(id: PublicKey)

  case class GetPeers()

  case class GetPeersID()


  case class GetId()

  case class GetBalance(account: PublicKey)

  case class HandShake(
                        originPeer: Signed[Peer],
                        peers: Seq[Signed[Peer]]
                      ) extends ProductHash

  case class HandShakeMessage(handShake: Signed[HandShake])

  case class HandShakeResponseMessage(hsr: Signed[HandShakeResponse])

  case class HandShakeResponse(
                              original: HandShake,
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

}

class PeerToPeer(
                  publicKey: PublicKey,
                  system: ActorSystem,
                  consensusActor: ActorRef,
                  udpActor: ActorRef,
                  selfAddress: InetSocketAddress = new InetSocketAddress("127.0.0.1", 16180),
                  keyPair: KeyPair = null
                )
                (implicit timeout: Timeout) extends Actor with ActorLogging {

  private val id = Id(publicKey)
  private implicit val kp: KeyPair = keyPair

  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher
  implicit val actorSystem: ActorSystem = context.system

  // @volatile private var peers: Set[InetSocketAddress] = Set.empty[InetSocketAddress]
  @volatile private var remotes: Set[InetSocketAddress] = Set.empty[InetSocketAddress]
  @volatile private var externalAddress: InetSocketAddress = selfAddress

  private val peerLookup = mutable.HashMap[InetSocketAddress, Signed[Peer]]()

  private def peerIDLookup = peerLookup.values.map{z => z.data.id -> z}.toMap

  private def selfPeer = Peer(id, externalAddress, remotes).signed()

  private def peerIPs = {
    peerLookup.keys ++ peerLookup.values.flatMap(z => z.data.remotes ++ Seq(z.data.externalAddress))
  }.toSet

  private def peers = peerLookup.values.toSeq

  def broadcast[T <: AnyRef](message: T): Unit = {
    peerIPs.foreach {
      peer => udpActor.udpSend(message, peer)
    }
  }

  private def handshake = HandShakeMessage(HandShake(selfPeer, peers).signed())


  def addPeer(p: PeerRef): StatusCode = {
    val peerAddress = p.address
    if (peerIPs.contains(peerAddress)) {
      log.debug(s"We already know $peerAddress, discarding")
      StatusCodes.AlreadyReported
    } else if (peerAddress == externalAddress || remotes.contains(peerAddress)) {
      log.debug(s"Peer is same as self $peerAddress, discarding")
      StatusCodes.BadRequest
    } else {
      log.debug(s"Sending handshake from $externalAddress to $peerAddress")
      //Introduce ourselves
      udpActor.udpSend(handshake, peerAddress)
      //Tell our existing peers
      broadcast(p)
      StatusCodes.Accepted
    }
  }

  private def addPeer(value: Signed[PeerToPeer.Peer], address: InetSocketAddress): Unit = {
    import akka.pattern.ask
    log.debug("Requesting BanList")
    val banList = (udpActor ? GetBanList).mapTo[Seq[InetSocketAddress]].get()
    log.debug("Requested BanList")
    if (!banList.contains(value.data.externalAddress)) {
      log.debug(s"Adding peer $address $value")
      addPeer(PeerRef(value.data.externalAddress))
      peerLookup(value.data.externalAddress) = value
      value.data.remotes.foreach(peerLookup(_) = value)
    } else{
      log.debug(s"Peer was banned! ${value.data.externalAddress}")
    }
    remotes += address
  }

  private def banOn[T](valid: => Boolean, remote: InetSocketAddress)(t: => T) = {
    if (valid) t else {
      log.debug(s"BANNING - Invalid HandShakeResponse from - $remote")
      udpActor ! Ban(remote)
    }
  }

  override def receive: Receive = {

    case GetExternalAddress() => sender() ! externalAddress

    case SetExternalAddress(addr) =>
      log.debug(s"Setting external address to $addr from RPC request")
      externalAddress = addr

    case AddPeerFromLocal(peerAddress) =>
      log.debug(s"Received a request to add peer $peerAddress")
      peerLookup.get(peerAddress) match {
        case Some(peer) =>
          log.debug(s"Disregarding request, already familiar with peer on $peerAddress - $peer")
          sender() ! StatusCodes.AlreadyReported
        case None =>
          sender() ! addPeer(PeerRef(peerAddress))
      }

    case GetPeers => sender() ! Peers(peerIPs.toSeq)

    case GetPeersID => sender() ! peers.map{_.data.id}

    case GetId =>
      sender() ! Id(publicKey)

    case UDPSendToID(data, remote) =>
      peerIDLookup.get(remote).foreach{
        r =>
          udpActor ! UDPSend(data, r.data.externalAddress)
      }


    case UDPMessage(p: PeerMemPoolUpdated, remote) =>
      consensusActor ! p

    case UDPMessage(p : PeerProposedBlock, remote) =>
      consensusActor ! p

    case UDPMessage(sh: HandShakeResponseMessage, remote) =>
      log.debug("HandShakeResponse missing")
      banOn(sh.hsr.valid, remote) {
        log.debug(s"Got valid HandShakeResponse from $remote")
        val hs = sh.hsr.data.response.originPeer
        addPeer(hs, remote)
      }

    case UDPMessage(sh: HandShakeMessage, remote) =>
      banOn(sh.handShake.valid, remote) {
        log.debug(s"Got valid handshake from $remote")
        val hs = sh.handShake.data
        val op = hs.originPeer
        addPeer(op, remote)
        val response = HandShakeResponseMessage(HandShakeResponse(sh.handShake.data, hs, remote).signed())
        udpActor.udpSend(response, remote)
      }

    case UDPMessage(peersI: Peers, remote) =>
      peersI.peers.foreach{
        p =>
          self ! PeerRef(p)
      }

    case UDPMessage(_: Terminated, remote) =>
      log.debug(s"Peer $remote has terminated. Removing it from the list.")
      // TODO: FIX
     // peerIPs -= remote

    case u: UDPMessage =>
      log.error(s"Unrecognized UDP message: $u")
  }

}
