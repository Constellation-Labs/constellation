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

  case class GetId()

  case class GetBalance(account: PublicKey)

  case class HandShake(
                      id: Id,
                      externalAddress: InetSocketAddress,
                      peers: Seq[Peer]
                      ) extends ProductHash

  case class HandShakeResponse(
                              original: HandShake,
                              response: HandShake
                              ) extends ProductHash

  case class SetExternalAddress(address: InetSocketAddress)

  case class GetExternalAddress()

  case class Peer(
                 id: Id,
                 externalAddress: InetSocketAddress,
                 remotes: Seq[InetSocketAddress] = Seq()
                 )

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
  private implicit val kp: KeyPair = KeyPair

  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher
  implicit val actorSystem: ActorSystem = context.system

  // @volatile private var peers: Set[InetSocketAddress] = Set.empty[InetSocketAddress]
  @volatile private var remotes: Set[InetSocketAddress] = Set.empty[InetSocketAddress]
  @volatile private var externalAddress: InetSocketAddress = selfAddress
  @volatile private val peerLookup = mutable.HashMap[InetSocketAddress, Peer]()

  private def peerIPs = {
    peerLookup.keys ++ peerLookup.values.flatMap(z => z.remotes ++ Seq(z.externalAddress))
  }.toSet

  private def peers = peerLookup.values.toSeq

  def broadcast[T <: AnyRef](message: T): Unit = {
    peerIPs.foreach {
      peer => udpActor.udpSend(message, peer)
    }
  }

  private def handshake = HandShake(id, externalAddress, peers)

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
      udpActor.udpSign(handshake, peerAddress)
      //Tell our existing peers
      broadcast(p)
      //Add to the current list of peers
      peerIPs += peerAddress
      StatusCodes.Accepted
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

    case GetId =>
      sender() ! Id(publicKey)

      // All these UDP Messages need to check if remote ip is banned.
    case UDPMessage(p: PeerRef, remote) =>
      self ! p

    case UDPMessage(p: PeerMemPoolUpdated, remote) =>
      consensusActor ! p

    case UDPMessage(p : PeerProposedBlock, remote) =>
      consensusActor ! p

    case UDPMessage(sh : Signed[HandShakeResponse], remote) =>
      if (sh.valid) {
        log.debug(s"Got valid HandShakeResponse from $remote")
        val hs = sh.data.response
        val peer = Peer(hs.id, hs.externalAddress, remotes = Seq(remote))
        peerLookup(hs.externalAddress) = peer
        peerLookup(remote) = peer
      } else {
        log.debug(s"BANNING - Invalid HandShakeResponse from - $remote")
        udpActor ! Ban(remote)
      }

    case UDPMessage(sh : Signed[HandShake], remote) =>
      // TODO: Make this a simple func above -- i.e. banResponse{valid}
      if (sh.valid) {
        log.debug(s"Got valid handshake from $remote")
        val p = Peer(sh.data.id, sh.data.externalAddress)
        val response = HandShakeResponse(sh.data, handshake)
        udpActor.udpSign(response, remote)
      } else {
        log.debug(s"BANNING - Invalid handshake from - $remote")
        udpActor ! Ban(remote)
      }
    case UDPMessage(peersI: Peers, remote) =>
      peersI.peers.foreach{
        p =>
          self ! PeerRef(p)
      }

    case UDPMessage(_: Terminated, remote) =>
      log.debug(s"Peer $remote has terminated. Removing it from the list.")
      peerIPs -= remote

    case u: UDPMessage =>
      log.error(s"Unrecognized UDP message: $u")
  }

}
