package org.constellation.p2p

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.constellation.util.Signed
import constellation._
import org.constellation.Data
import org.constellation.consensus.Consensus.RemoteMessage

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Try}
import org.constellation.primitives.Schema._

trait PeerAuth {

  val data: Data
  import data._
  val udpActor: ActorRef
  var requestExternalAddressCheck: Boolean
  val self: ActorRef
  val logger: Logger
  implicit val timeout: Timeout
  implicit val executionContext: ExecutionContextExecutor
  implicit val actorSystem: ActorSystem

  def broadcast[T <: RemoteMessage](message: T, skipIDs: Seq[Id] = Seq(), idSubset: Seq[Id] = Seq()): Unit = {
    val dest: Iterable[Id] = if (idSubset.isEmpty) peerIDLookup.keys else idSubset

    dest.foreach{ i =>
      if (!skipIDs.contains(i)) {
        totalNumBroadcastMessages += 1
        self ! UDPSend(message, peerIDLookup(i).data.externalAddress)
      }
    }
  }

  def handShakeInner: HandShake = {
    HandShake(selfPeer, requestExternalAddressCheck) //, peers)
  }

  def initiatePeerHandshake(peerAddress: InetSocketAddress): StatusCode = {
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

        udpActor ! UDPSend(message, peerAddress)
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

  def addPeer(
               value: Signed[Peer],
               newPeers: Seq[Signed[Peer]] = Seq()
             ): Unit = {

    this.synchronized {
      peerLookup(value.data.externalAddress) = value
      value.data.remotes.foreach(peerLookup(_) = value)
      logger.debug(s"Peer added, total peers: ${peerIDLookup.keys.size} on ${id.short}")
      newPeers.foreach { np =>
        //    logger.debug(s"Attempting to add new peer from peer reference handshake response $np")
        //   initiatePeerHandshake(PeerRef(np.data.externalAddress))
      }
    }
  }

  def banOn[T](valid: => Boolean, remote: InetSocketAddress)(t: => T): Unit = {
    if (valid) t else {
      logger.debug(s"BANNING - Invalid data from - $remote")
      udpActor ! Ban(remote)
    }
  }

  def handleHandShake(sh: HandShakeMessage, remote: InetSocketAddress): Unit = {
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

      udpActor ! UDPSend(response, responseAddr)

      //Tell our existing peers
      initiatePeerHandshake(responseAddr)
    }
  }

  def handleHandShakeResponse(sh: HandShakeResponseMessage, remote: InetSocketAddress): Unit = {
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
      peerLookup(remote) = value
    //  remotes += remote
    }
  }

  def addPeerFromLocal(peerAddress: InetSocketAddress): StatusCode = {
    logger.debug(s"AddPeerFromLocal inet: ${pprintInet(peerAddress)}")

    peerLookup.get(peerAddress) match {
      case Some(peer) =>
        logger.debug(s"Disregarding request, already familiar with peer on $peerAddress - $peer")
        StatusCodes.AlreadyReported
      case None =>
        logger.debug(s"Peer $peerAddress unrecognized, adding peer")
        val attempt = Try {
          initiatePeerHandshake(peerAddress)
        }
        attempt match {
          case Failure(e) => e.printStackTrace(
          )
          case _ =>
        }

        val code = attempt.getOrElse(StatusCodes.InternalServerError)
        code
    }

  }

  // TODO: Send other peers termination message on shutdown.


}
