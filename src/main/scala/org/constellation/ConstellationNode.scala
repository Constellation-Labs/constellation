package org.constellation

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.constellation.rpc.RPCInterface
import org.constellation.wallet.KeyUtils
import org.constellation.consensus.Consensus
import org.constellation.p2p.PeerToPeer
import org.constellation.p2p.PeerToPeer.AddPeer
import org.constellation.state.{ChainStateManager, MemPoolManager}

import scala.concurrent.ExecutionContextExecutor

object ConstellationNode extends App {

  implicit val system: ActorSystem = ActorSystem("Constellation")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val config = ConfigFactory.load()

  val rpcTimeout = config.getInt("blockchain.defaultRPCTimeoutSeconds")

  // TODO: update to take from config
  val keyPair = KeyUtils.makeKeyPair()

  // TODO: add seeds from config
  new ConstellationNode(keyPair, null, config.getString("http.interface"), config.getInt("http.port"), rpcTimeout)
}

class ConstellationNode(
               val keyPair: KeyPair,
               val seedHost: String,
               val httpInterface: String,
               val httpPort: Int,
               timeoutSeconds: Int = 5,
               actorNameSuffix: String = ""
             )(
               implicit val system: ActorSystem,
               implicit val materialize: ActorMaterializer,
               implicit val executionContext: ExecutionContextExecutor
             ) extends RPCInterface {
  val logger = Logger("ConstellationNode_" + actorNameSuffix)

  override implicit val timeout: Timeout = Timeout(timeoutSeconds, TimeUnit.SECONDS)

  // TODO: add root actor for routing

  // Setup actors
  val peerToPeerActor: ActorRef = system.actorOf(Props(new PeerToPeer), "ConstellationPeerToPeerActor")

  val memPoolManagerActor: ActorRef = system.actorOf(Props(new MemPoolManager), "ConstellationPeerToPeerActor")

  val chainStateActor: ActorRef = system.actorOf(Props(new ChainStateManager), "ConstellationPeerToPeerActor")

  val consensusActor: ActorRef = system.actorOf(
    Props(new Consensus(memPoolManagerActor, chainStateActor, keyPair)), "ConstellationConsensusActor")

  // Seed peers
  if (seedHost.nonEmpty) {
    logger.info(s"Attempting to connect to seed-host $seedHost")
    peerToPeerActor ! AddPeer(seedHost)
  } else {
    logger.info("No seed host configured, waiting for messages.")
  }

  // Setup http server for rpc
  Http().bindAndHandle(routes, httpInterface, httpPort)
}
