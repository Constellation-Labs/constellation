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
    val dest: Iterable[Id] = if (idSubset.isEmpty) signedPeerIDLookup.keys else idSubset

    dest.foreach{ i =>
      if (!skipIDs.contains(i)) {
        totalNumBroadcastMessages += 1

        val address = signedPeerIDLookup(i).data.externalAddress

        address.foreach{ a =>
          udpActor ! UDPSend(message, a)
        }
      }
    }
  }

  def handShakeInner(peerAddressOrRemote: InetSocketAddress): HandShake = {
    HandShake(selfPeer, peerAddressOrRemote, peers.toSet,  requestExternalAddressCheck)
  }

  def initiatePeerHandshake(peerAddress: InetSocketAddress, useRest: Boolean = false): StatusCode = {
    val banList = data.bannedIPs

    if (!banList.contains(peerAddress)) {

      val res = if (peerIPs.contains(peerAddress)) {
        logger.debug(s"We already know $peerAddress, discarding")

        StatusCodes.AlreadyReported
      } else if (externalAddress.contains(peerAddress) || remotes.contains(peerAddress)) {
        logger.debug(s"Peer is same as self $peerAddress, discarding")

        StatusCodes.BadRequest
      } else {
        logger.debug(s"Sending handshake from $externalAddress to $peerAddress with ${peers.size} known peers")

        //Introduce ourselves
        val message = HandShakeMessage(handShakeInner(peerAddress).signed())

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

  def addAuthenticatedPeer(value: Signed[Peer], newPeers: Seq[Signed[Peer]] = Seq()): Unit = {

    value.data.externalAddress.foreach{
      a =>

      signedPeerLookup(a) = value

      value.data.remotes.foreach{ r =>
        signedPeerLookup(r) = value
        addressToLastObservedExternalAddress(r) = a
      }

      logger.debug(s"Peer added, total peers: ${signedPeerIDLookup.keys.size} on ${id.short}")
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
    val responseAddr = if (hs.requestExternalAddressCheck) remote else address.getOrElse(remote)

    logger.debug(s"Got handshake from $remote on $externalAddress, sending response to $responseAddr")

    banOn(sh.handShake.valid, remote) {

      logger.debug(s"Got handshake inner from $remote on $externalAddress, " +
        s"sending response to $remote inet: ${pprintInet(remote)} " +
        s"peers externally reported address: ${hs.originPeer.data.externalAddress} inet: " +
        s"${address.map{pprintInet}}")

      val lastExternal = if (address.nonEmpty) None else addressToLastObservedExternalAddress.get(remote)

      val response = HandShakeResponseMessage(
        HandShakeResponse(sh.handShake, handShakeInner(remote), lastExternal).signed()
      )

      udpActor ! UDPSend(response, remote)

      //Tell our existing peers
      initiatePeerHandshake(responseAddr)
    }
  }

  def handleHandShakeResponse(sh: HandShakeResponseMessage, remote: InetSocketAddress): Unit = {
    val hsr = sh.handShakeResponse.data
    val address = hsr.response.originPeer.data.externalAddress

    if (requestExternalAddressCheck) {
      externalAddress = Some(hsr.response.destination)
      requestExternalAddressCheck = false
    }

    // For node restart
    if (externalAddress.isEmpty){
      hsr.lastObservedExternalAddress.foreach{ e =>
        externalAddress = Some(e)
        if (apiAddress.isEmpty) {
          apiAddress = Some(new InetSocketAddress(e.getHostString, e.getPort))
        }
      }
    }

    // ^ TODO : Fix validation
    banOn(sh.handShakeResponse.valid, remote) {
      logger.debug(s"Got valid HandShakeResponse from $remote / $address on $externalAddress")

      val value = hsr.response.originPeer
      val newPeers = Seq()

      addAuthenticatedPeer(value, newPeers)

      signedPeerLookup(remote) = value
    }
  }

  def addPeerFromLocal(peerAddress: InetSocketAddress, knownId: Option[Id] = None): StatusCode = {
    logger.debug(s"AddPeerFromLocal inet: ${pprintInet(peerAddress)}")

    signedPeerLookup.get(peerAddress) match {
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
