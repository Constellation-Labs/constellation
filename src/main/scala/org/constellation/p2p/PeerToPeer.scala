package org.constellation.p2p

import java.security.PublicKey

import akka.NotUsed
import akka.actor.Status.Success
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.constellation.{Cell, Data, Sheaf}
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
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val logger = Logger(s"PeerToPeer")

  override def receive: Receive = {

    // Local commands

    case tx: TX => handleLocalTransactionAdd(tx)

    case AddPeerFromLocal(peerAddress) => sender() ! addPeerFromLocal(peerAddress)

    case UDPSendToID(dataA, remoteId) =>
      peerIDLookup.get(remoteId).foreach{
        r =>
          udpActor ! UDPSendTyped(dataA, r.data.externalAddress)
      }

    // Regular state checks

    case InternalHeartbeat =>

      processHeartbeat {

        downloadHeartbeat()

        val numAccepted = gossipHeartbeat()
        /*
                logger.debug(
                  s"Heartbeat: ${id.short}, " +
                    s"bundles: $totalNumBundleMessages, " +
                    s"broadcasts: $totalNumBroadcastMessages, " +
                    s"numBundles: ${bundles.size}, " +
                    s"gossip: $totalNumGossipMessages, " +
                    s"balance: $selfBalance, " +
                    s"memPool: ${memPoolTX.size} numPeers: ${peers.size} " +
                    s"numAccepted: $numAccepted, numTotalValid: ${validTX.size} " +
                    s"validUTXO: ${validLedger.map { case (k, v) => k.slice(0, 5) -> v }} " +
                    s"peers: ${peers.map { p =>
                      p.data.id.short + "-" + p.data.externalAddress + "-" + p.data.remotes
                    }.mkString(",")}"
                )

                */
      }

    // Peer messages

    case UDPMessage(message: Any, remote) =>

      message match {

        case d: DownloadRequest => handleDownloadRequest(d, remote)

        case d: DownloadResponse => handleDownloadResponse(d)

        case sh: HandShakeMessage => handleHandShake(sh, remote)

        case sh: HandShakeResponseMessage => handleHandShakeResponse(sh, remote)

        case message: RemoteMessage => consensusActor ! message

        // case g @ Gossip(_) => handleGossip(g, remote)
        case gm : GossipMessage => handleGossip(gm, remote)

        // Deprecated
        case t: AddTransaction => memPoolActor ! t
        case sheaf: Sheaf => ring ! sheaf
        case u =>
          logger.error(s"Unrecognized UDP message: $u")
      }


    // Deprecated below

    case a @ AddTransaction(transaction) =>
      logger.debug(s"Broadcasting TX ${transaction.short} on ${id.short}")
      broadcast(a)

  }

  /**
    * Pipes messages sent to ActorRef into async buffer, will need mapAsync when returning futures (ask's to chain state manager)
    */
  val buffer = Source.actorRef[Sheaf](100, OverflowStrategy.dropNew)
  val liftedEvents = buffer.map(embed)
  val ring = Flow[Sheaf].to(Sink.actorRef(consensusActor, "bogus")).runWith(liftedEvents)

  def embed(event: Sheaf): Sheaf = Cell.ioF(Sheaf())
}
