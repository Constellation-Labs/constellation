package org.constellation.p2p

import java.security.PublicKey
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Terminated}
import akka.pattern.pipe
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.constellation.actor.Receiver
import org.constellation.p2p.PeerToPeer._
import org.constellation.rpc.ProtocolInterface.GetChain

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration

object PeerToPeer {

  case class AddPeer(address: String)

  case class PeerRef(actorRef: ActorRef)

  case class Peers(peers: Seq[String])

  case class Id(id: PublicKey)

  case class GetPeers()

  case class GetId()

  case class GetBalance(account: PublicKey)

  case class HandShake()
}

trait PeerToPeer {
  this: Receiver =>

  implicit val timeout: Timeout = Timeout(Duration(5, TimeUnit.SECONDS))
  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher

  val logger: Logger
  val peers: scala.collection.mutable.Set[ActorRef] = scala.collection.mutable.Set.empty[ActorRef]

  def broadcast(message: Any ): Unit = {
    peers.foreach {  peer => peer ! message
      //logger.info(s"just broadcasted $message to $peer")
    }
  }

  def broadcastAsk(message: Any): Seq[Future[Any]] = {
    import akka.pattern.ask
    peers.map{p => p ? message}.toSeq
  }

  receiver {

    case AddPeer(peerAddress) =>
      logger.debug(s"Got request to add peer $peerAddress")
      /*
      adds peer to actor system, res is a future of actor ref, sends the actor ref back to this actor, handshake occurs below
       */
      context.actorSelection(peerAddress).resolveOne().map( PeerRef(_) ).pipeTo(self)
    case PeerRef(newPeerRef: ActorRef) =>

      if (!peers.contains(newPeerRef)){
        context.watch(newPeerRef)
        logger.debug(s"Watching $newPeerRef}")

        //Introduce ourselves
        newPeerRef ! HandShake
        logger.debug(s"HandShake $newPeerRef}")

        //Ask for its friends
        newPeerRef ! GetPeers
        logger.debug(s"GetPeers $newPeerRef}")

        //Tell our existing peers
        broadcast(AddPeer(newPeerRef.path.toSerializationFormat))

        //Add to the current list of peers
        peers += newPeerRef

        newPeerRef ! GetChain


      } else logger.debug("We already know this peer, discarding")

    case Peers(peersI) => peersI.foreach( self ! AddPeer(_))

    case HandShake =>
      logger.debug(s"Received a handshake from ${sender().path.toStringWithoutAddress}")
      peers += sender()

    case GetPeers => sender() ! Peers(peers.toSeq.map(_.path.toSerializationFormat))

    case Terminated(actorRef) =>
      logger.debug(s"Peer ${actorRef} has terminated. Removing it from the list.")
      peers -= actorRef

  }

}
