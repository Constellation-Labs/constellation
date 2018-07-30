package org.constellation.p2p

import java.security.PublicKey

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.constellation.Data
import org.constellation.consensus.Consensus._
import org.constellation.primitives.Schema.{Transaction, _}
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
                  val heartbeatEnabled: Boolean = false,
                  randomTransactionManager: ActorRef,
                  cellManager: ActorRef
                )
                (implicit timeoutI: Timeout, materialize: ActorMaterializer) extends Actor
  with ActorLogging
  with PeerAuth
  with Heartbeat
  with ProbabilisticGossip
  with Checkpoint
  with Download {

  import data._

  implicit val timeout: Timeout = timeoutI
  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher
  implicit val actorMaterializer: ActorMaterializer = materialize
  implicit val actorSystem: ActorSystem = context.system

  val logger = Logger(s"PeerToPeer")

  var lastTPSCheckTime: Long = System.currentTimeMillis()
  var lastNumValidTX: Long = 0L
  // Check TPS every 100 seconds
  val tpsCheckIntervalSeconds = 100

  def tpsCalculate(): Unit = {
    if (System.currentTimeMillis() > (lastTPSCheckTime + (tpsCheckIntervalSeconds*1000))) {
      lastTPSCheckTime = System.currentTimeMillis()
      val delta = Math.max(data.totalNumValidatedTX - lastNumValidTX, 1) // safety for divide by zero
      transactionsPerSecond = delta.toDouble / 100
      lastNumValidTX = data.totalNumValidatedTX
    }
  }

  override def receive: Receive = {

    // Local commands
    case AddPeerFromLocal(peerAddress) => sender() ! addPeerFromLocal(peerAddress)

    // Regular state checks
    case InternalHeartbeat =>

      tpsCalculate()

      if (sendRandomTXV2) {
        randomTransactionManager ! InternalHeartbeat
        cellManager ! InternalHeartbeat
      }

      processHeartbeat {

        if (heartbeatRound % 3 == 0) {

          // Attempt add peers
          peersAwaitingAuthenticationToNumAttempts.foreach {
            case (peerAddr, attempts) =>

              val added = signedPeerLookup.contains(peerAddr)
              val giveUp = attempts > 10

              if (giveUp || added) {
                if (added) logger.debug("Peer awaiting authenticated has been accepted")
                if (giveUp) logger.debug(s"Giving up on adding $peerAddr exceeded max attempts 10")

                peersAwaitingAuthenticationToNumAttempts.remove(peerAddr)
              } else {
                val res = addPeerFromLocal(peerAddr)

                if (res == StatusCodes.OK) peersAwaitingAuthenticationToNumAttempts.remove(peerAddr)
                else {
                  peersAwaitingAuthenticationToNumAttempts(peerAddr) =
                    peersAwaitingAuthenticationToNumAttempts(peerAddr) + 1
                }
              }
          }



          // Remove dead peers
          lastPeerRX.foreach{ case (id, rx) =>
            if (rx < (System.currentTimeMillis() - 120000)) {
              signedPeerLookup.filter(_._2.data.id == id).foreach{ case (p, a) =>
                signedPeerLookup.remove(p)
                peersAwaitingAuthenticationToNumAttempts.remove(p)
                peerSync.remove(id)
                a.data.externalAddress.foreach{deadPeers :+= _}
              }
            }
          }


        }


        if (heartbeatRound % 120 == 0) {
          deadPeers.foreach(addPeerFromLocal(_))
        }


        downloadHeartbeat()

        heartbeatRound += 1

        // checkpointHeartbeat()

        gossipHeartbeat()

        if (heartbeatRound % 30 == 0) {
          logger.debug(
            s"Heartbeat: ${id.short}, " +
              s"numActiveBundles: ${activeDAGBundles.size}, " +
              s"maxHeight: ${maxBundle.flatMap{_.meta.map{_.height}}}, " +
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

    case g: GossipMessage =>
      handleGossip(g, null)

    case UDPMessage(message: Any, remote) =>

      totalNumP2PMessages += 1

      val authenticated = signedPeerLookup.contains(remote)
/*
      if (!authenticated && !peersAwaitingAuthenticationToNumAttempts.contains(remote)) {
        logger.debug("Attempting to add unauthenticated peer")
        peersAwaitingAuthenticationToNumAttempts(remote) = 1
      }
*/

      if (authenticated) {
        val remoteId = signedPeerLookup(remote).data.id

        lastPeerRX(remoteId) = System.currentTimeMillis()

        message match {
          case gm: GossipMessage => handleGossip(gm, remote)

          case sh: HandShakeMessage => handleHandShake(sh, remote)

          case sh: HandShakeResponseMessage => handleHandShakeResponse(sh, remote)

          case u => logger.error(s"Unrecognized authenticated UDP message: $u")
        }
      } else {

        message match {
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

          case u =>  logger.error(s"Unrecognized UDP message: $u - authenticated: $authenticated")

        }
      }
  }
}

