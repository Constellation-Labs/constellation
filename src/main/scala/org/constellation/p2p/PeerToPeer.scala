package org.constellation.p2p

import java.net.InetSocketAddress
import java.security.PublicKey

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Terminated}
import akka.pattern.pipe
import akka.util.Timeout
import org.constellation.consensus.Consensus.{PeerMemPoolUpdated, PeerProposedBlock}
import org.constellation.p2p.PeerToPeer._
import constellation._

import scala.concurrent.{ExecutionContextExecutor, Future}

object PeerToPeer {

  case class AddPeerFromLocal(address: InetSocketAddress)

  case class PeerRef(address: InetSocketAddress)

  case class Peers(peers: Seq[InetSocketAddress])

  case class Id(id: PublicKey)

  case class GetPeers()

  case class GetPeerActorRefs()

  case class GetId()

  case class GetBalance(account: PublicKey)

  case class HandShake()
}

import akka.pattern.ask

class PeerToPeer(
                  publicKey: PublicKey,
                  system: ActorSystem,
                  consensusActor: ActorRef,
                  udpActor: ActorRef,
                  selfAddress: InetSocketAddress = new InetSocketAddress("127.0.0.1", 16180)
                )
                (implicit timeout: Timeout) extends Actor with ActorLogging {

  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher
  implicit val actorSystem: ActorSystem = context.system

  var peers: Set[InetSocketAddress] = Set.empty[InetSocketAddress]

  def broadcast[T <: AnyRef](message: T): Unit = {
    peers.foreach {
      peer => udpActor.udpSend(message, peer)
    }
  }

  override def receive: Receive = {

    case AddPeerFromLocal(peerAddress) =>
      log.debug(s"Received a request to add peer $peerAddress")
      self ! PeerRef(peerAddress)

    case p @ PeerRef(peerAddress) =>
      // TODO: Add validation that the peer reference isn't banned

      if (!peers.contains(peerAddress) && peerAddress != selfAddress){

        log.debug(s"Sending handshake from $selfAddress to $peerAddress")
        //Introduce ourselves
        udpActor.udpSend(HandShake(), peerAddress)

        //Tell our existing peers
        broadcast(p)

        //Add to the current list of peers
        peers += peerAddress

      } else log.debug(s"We already know $peerAddress, discarding")

    case GetPeers => sender() ! Peers(peers.toSeq)

    case GetId =>
      sender() ! Id(publicKey)

      // All these UDP Messages need to check if remote ip is banned.
    case UDPMessage(p: PeerRef, remote) =>
      self ! p

    case UDPMessage(p: PeerMemPoolUpdated, remote) =>
      consensusActor ! p

    case UDPMessage(p : PeerProposedBlock, remote) =>
      consensusActor ! p

    case UDPMessage(_ : HandShake, remote) =>
      log.debug(s"Got handshake from $remote")
      self ! PeerRef(remote)
      udpActor.udpSend(Peers(peers.toSeq), remote)

    case UDPMessage(peersI: Peers, remote) =>
      peersI.peers.foreach{
        p =>
          self ! PeerRef(p)
      }

    case UDPMessage(_: Terminated, remote) =>
      log.debug(s"Peer $remote has terminated. Removing it from the list.")
      peers -= remote

    case u: UDPMessage =>
      log.error(s"Unrecognized UDP message: $u")
  }

}
