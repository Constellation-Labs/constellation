package org.constellation.p2p

import java.security.PublicKey

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.constellation.Data
import org.constellation.consensus.Consensus._
import org.constellation.primitives.Schema.{TX, _}
import org.constellation.util.Heartbeat
import constellation._

import scala.concurrent.ExecutionContextExecutor

class PeerToPeer(
                  val publicKey: PublicKey,
                  system: ActorSystem,
                  val consensusActor: ActorRef,
                  val udpActor: ActorRef,
                  val data: Data = null,
                  var requestExternalAddressCheck: Boolean = false,
                  val heartbeatEnabled: Boolean = false
                )
                (implicit timeoutI: Timeout) extends Actor
  with ActorLogging
  with PeerAuth
  with Heartbeat
  with ProbabilisticGossip
  with Checkpoint
  with Download {

  import data._

  makeKeyPair()

  implicit val timeout: Timeout = timeoutI
  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher
  implicit val actorSystem: ActorSystem = context.system

  val logger = Logger(s"PeerToPeer")

  override def receive: Receive = {

    // Local commands

    case AddPeerFromLocal(peerAddress) => sender() ! addPeerFromLocal(peerAddress)

    case UDPSendToID(dataA, remoteId) =>
      peerIDLookup.get(remoteId).foreach{
        r =>
          udpActor ! UDPSendTyped(dataA, r.data.externalAddress)
      }

    // Regular state checks

    case InternalHeartbeat =>

      processHeartbeat {

        if (heartbeatRound % 3 == 0) {
          peersAwaitingAuthenticationToNumAttempts.foreach {
            case (peerAddr, attempts) =>
              if (attempts > 10 || peerLookup.contains(peerAddr))
                peersAwaitingAuthenticationToNumAttempts.remove(peerAddr)
              else {
                val res = addPeerFromLocal(peerAddr)
                if (res == StatusCodes.OK) peersAwaitingAuthenticationToNumAttempts.remove(peerAddr)
                else {
                  peersAwaitingAuthenticationToNumAttempts(peerAddr) =
                    peersAwaitingAuthenticationToNumAttempts(peerAddr) + 1
                }
              }
          }
        }

        downloadHeartbeat()
        heartbeatRound += 1

     //   checkpointHeartbeat()

        gossipHeartbeat()

        if (heartbeatRound % 30 == 0) {
          logger.debug(
            s"Heartbeat: ${id.short}, " +
              s"numActiveBundles: ${activeDAGBundles.size}, " +
              s"maxHeight: ${Option(maxBundle).flatMap{_.meta.map{_.height}}}, " +
              s"numTotalValidTX: $totalNumValidatedTX " +
              s"numPeers: ${peers.size} " +
              s"numBundleMessages: $totalNumBundleMessages, " +
              s"broadcasts: $totalNumBroadcastMessages, " +
              s"numGossipMessages: $totalNumGossipMessages, " +
              s"balance: $selfBalance, " +
              s"memPool: ${memPool.size} numPeers: ${peers.size} " +
              s"validUTXO: ${validLedger.map { case (k, v) => k.slice(0, 5) -> v }} " +
              ""
          )
        }

      }

    // Peer messages

    case UDPMessage(message: Any, remote) =>



      message match {

        case d: DownloadRequest => handleDownloadRequest(d, remote)

        case d: DownloadResponse => handleDownloadResponse(d)

        case sh: HandShakeMessage => handleHandShake(sh, remote)

        case sh: HandShakeResponseMessage => handleHandShakeResponse(sh, remote)

/*
        case m @ StartConsensusRound(id, voteData, roundHash) => {
          voteData match {
            case CheckpointVote(d) =>
              consensusActor ! ConsensusVote(id, voteData, roundHash)
              logger.debug(s"received checkpoint start consensus round message roundHash= $roundHash, self = $publicKey id = $id")
            case ConflictVote(d) =>
              logger.debug(s"received conflict start consensus round message = $m")
          }
        }

        case message: RemoteMessage => consensusActor ! message
*/

        // case g @ Gossip(_) => handleGossip(g, remote)
        case gm : GossipMessage =>
          handleGossip(gm, remote)

        case u =>
          logger.error(s"Unrecognized UDP message: $u")
      }

  }

}

