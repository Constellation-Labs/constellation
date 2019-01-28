package org.constellation.p2p

import java.net.InetSocketAddress
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.softwaremill.sttp.Response
import com.typesafe.scalalogging.Logger

import constellation._
import org.constellation.DAO
import org.constellation.consensus.Consensus.RemoteMessage
import org.constellation.primitives.Schema._
import org.constellation.util.{APIClient, Signed}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Try}

// Don't use this yet. // tmp comment

/** @todo Needs to be merged with other peer auth flow and updated substantially. */
trait PeerAuth {

  val data: DAO

  import data._

  val udpActor: ActorRef
  var requestExternalAddressCheck: Boolean
  val self: ActorRef
  val logger: Logger
  implicit val timeout: Timeout
  implicit val executionContext: ExecutionContextExecutor
  implicit val actorMaterializer: ActorMaterializer
  implicit val actorSystem: ActorSystem

  // doc
  def getBroadcastTCP(skipIDs: Seq[Id] = Seq(),
                      idSubset: Seq[Id] = Seq(),
                      route: String): Seq[(InetSocketAddress, Future[Response[String]])] = {
    val addresses = getBroadcastPeers(skipIDs, idSubset).map(_.apiAddress)

    addresses.map(a => {
      val address = a.get
      val hostName = address.getHostName
      val port = address.getPort

      val client = APIClient(hostName, port)

      address -> client.get(route)
    })
  }

  // doc
  def broadcastUDP[T <: RemoteMessage](message: T, skipIDs: Seq[Id] = Seq(), idSubset: Seq[Id] = Seq()): Unit = {
    getBroadcastPeers(skipIDs, idSubset).map(_.externalAddress).foreach(a => {
      val address = a.get
      udpActor ! UDPSend(message, address)
    })
  }

  // doc
  def getBroadcastPeers(skipIDs: Seq[Id] = Seq(), idSubset: Seq[Id] = Seq()): Seq[Peer] = {
    val peers: Iterable[Id] = if (idSubset.isEmpty) signedPeerIDLookup.keys else idSubset

    peers.filter(!skipIDs.contains(_)).map(p => {
      signedPeerIDLookup(p).data
    }).toSeq
  }

  // doc
  def apiBroadcast[T](f: APIClient => T, skipIDs: Seq[Id] = Seq()): Iterable[T] = {
    signedPeerIDLookup.keys.filterNot {
      skipIDs.contains
    }.flatMap {
      i =>
        getOrElseUpdateAPIClient(i).map {
          a =>
            // println("API broadcast to " + a.host + " " + a.port)
            f(a)
        }
    }
  }

  // doc
  def broadcast[T <: RemoteMessage](message: T, skipIDs: Seq[Id] = Seq(), idSubset: Seq[Id] = Seq()): Unit = {
    val dest: Iterable[Id] = if (idSubset.isEmpty) signedPeerIDLookup.keys else idSubset

    dest.foreach { i =>
      if (!skipIDs.contains(i)) {

        val address = signedPeerIDLookup(i).data.externalAddress

        address.foreach { a =>
          udpActor ! UDPSend(message, a)
        }
      }
    }
  }

  // doc
  def handShakeInner(peerAddressOrRemote: InetSocketAddress): HandShake = {
    HandShake(selfPeer, peerAddressOrRemote, peers.toSet, requestExternalAddressCheck)
  }

  // doc
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

        StatusCodes.Accepted
      }

      res
    } else {
      logger.debug(s"Attempted to add peer but peer was previously banned! $peerAddress")
      StatusCodes.Forbidden
    }
  }

  // doc
  def addAuthenticatedPeer(value: Signed[Peer], newPeers: Seq[Signed[Peer]] = Seq()): Unit = {

    value.data.externalAddress.foreach {
      a =>

        signedPeerLookup(a) = value

        value.data.remotes.foreach { r =>
          signedPeerLookup(r) = value
          addressToLastObservedExternalAddress(r) = a
        }

        logger.debug(s"Peer added, total peers: ${signedPeerIDLookup.keys.size} on ${id.short}")

        Future {
          getOrElseUpdateAPIClient(value.id)
        }
    }
  }

  // doc
  def banOn[T](valid: => Boolean, remote: InetSocketAddress)(t: => T): Unit = {
    if (valid) t else {
      logger.debug(s"BANNING - Invalid data from - $remote")
      udpActor ! Ban(remote)
    }
  }

  // doc
  def handleHandShake(sh: HandShakeMessage, remote: InetSocketAddress): Unit = {
    val hs = sh.handShake.data
    val address = hs.originPeer.data.externalAddress
    val responseAddr = if (hs.requestExternalAddressCheck) remote else address.getOrElse(remote)

    logger.debug(s"Got handshake from $remote on $externalAddress, sending response to $responseAddr")

    banOn(sh.handShake.valid, remote) {

      logger.debug(s"Got handshake inner from $remote on $externalAddress, " +
        s"sending response to $remote inet: ${pprintInet(remote)} " +
        s"peers externally reported address: ${hs.originPeer.data.externalAddress} inet: " +
        s"${
          address.map {
            pprintInet
          }
        }")

      val lastExternal = if (address.nonEmpty) None else addressToLastObservedExternalAddress.get(remote)

      val response = HandShakeResponseMessage(
        HandShakeResponse(sh.handShake, handShakeInner(remote), lastExternal).signed()
      )

      udpActor ! UDPSend(response, remote)

      //Tell our existing peers
      initiatePeerHandshake(responseAddr)
    }
  }

  // doc
  def handleHandShakeResponse(sh: HandShakeResponseMessage, remote: InetSocketAddress): Unit = {
    val hsr = sh.handShakeResponse.data
    val address = hsr.response.originPeer.data.externalAddress

    if (requestExternalAddressCheck) {
      externalAddress = Some(hsr.response.destination)
      requestExternalAddressCheck = false
    }

    // For node restart
    if (externalAddress.isEmpty) {
      hsr.lastObservedExternalAddress.foreach { e =>
        externalAddress = Some(e)
        if (apiAddress.isEmpty) {
          apiAddress = Some(new InetSocketAddress(e.getHostString, e.getPort))
        }
      }
    }

    // ^ TODO : Fix validation // tmp comment

    banOn(sh.handShakeResponse.valid, remote) {
      logger.debug(s"Got valid HandShakeResponse from $remote / $address on $externalAddress")

      val value = hsr.response.originPeer
      val newPeers = Seq()

      addAuthenticatedPeer(value, newPeers)

      signedPeerLookup(remote) = value
    }
  }

  // doc
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

  // TODO: Send other peers termination message on shutdown. // tmp comment

} // end PeerAuth trait
