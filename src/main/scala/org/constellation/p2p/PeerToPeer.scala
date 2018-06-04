package org.constellation.p2p

import java.security.PublicKey

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.constellation.Data
import org.constellation.consensus.Consensus._
import org.constellation.primitives.Schema.{TX, _}
import org.constellation.state.MemPoolManager.AddTransaction
import org.constellation.util.Heartbeat

import scala.concurrent.ExecutionContextExecutor

class PeerToPeer(
                  publicKey: PublicKey,
                  system: ActorSystem,
                  consensusActor: ActorRef,
                  val udpActor: ActorRef,
                  val data: Data = null,
                  chainStateActor : ActorRef = null,
                  memPoolActor : ActorRef = null,
                  var requestExternalAddressCheck: Boolean = false,
                  val heartbeatEnabled: Boolean = false
                )
                (implicit timeoutI: Timeout) extends Actor
  with ActorLogging
  with PeerAuth
  with Heartbeat
  with ProbabilisticGossip
  with Download {

  import data._

  implicit val timeout: Timeout = timeoutI
  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher
  implicit val actorSystem: ActorSystem = context.system

  val logger = Logger(s"PeerToPeer")

  override def receive: Receive = {

    case InternalHeartbeat =>

      processHeartbeat {

        downloadHeartbeat()

        val numAccepted = gossipHeartbeat()

        logger.debug(
          s"Heartbeat: ${id.short}, memPool: ${memPoolTX.size} numPeers: ${peers.size} gossip: $totalNumGossipMessages, balance: $selfBalance, " +
            s"numAccepted: $numAccepted, numTotalValid: ${validTX.size} " +
            s"validUTXO: ${validUTXO.map { case (k, v) => k.slice(0, 5) -> v }} " +
            s"peers: ${peers.map { p => p.data.id.short + "-" + p.data.externalAddress + "-" + p.data.remotes }.mkString(",")}"
        )
      }

    case UDPMessage(message: Any, remote) =>

      message match {

        case d: DownloadRequest => handleDownloadRequest(d, remote)

        case d: DownloadResponse => handleDownloadResponse(d)

        case sh: HandShakeMessage => handleHandShake(sh, remote)

        case sh: HandShakeResponseMessage => handleHandShakeResponse(sh, remote)

        case message: RemoteMessage => consensusActor ! message

        case u =>
          logger.error(s"Unrecognized UDP message: $u")
      }

    case tx: TX => handleLocalTransactionAdd(tx)

    case AddPeerFromLocal(peerAddress) => sender() ! addPeerFromLocal(peerAddress)

    case UDPSendToID(dataA, remoteId) =>
      peerIDLookup.get(remoteId).foreach{
        r =>
          udpActor ! UDPSendTyped(dataA, r.data.externalAddress)
      }


    // Deprecated below

    case a @ AddTransaction(transaction) =>
      logger.debug(s"Broadcasting TX ${transaction.short} on ${id.short}")
      broadcast(a)

    case UDPMessage(t: AddTransaction, remote) =>
      memPoolActor ! t

  }

}

