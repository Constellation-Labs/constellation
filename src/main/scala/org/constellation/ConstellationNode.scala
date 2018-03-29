package org.constellation

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.constellation.rpc.RPCInterface
import org.constellation.wallet.KeyUtils
import org.constellation.consensus.Consensus
import org.constellation.consensus.Consensus.Initialize
import org.constellation.p2p.PeerToPeer
import org.constellation.p2p.PeerToPeer.{AddPeer, GetPeers}
import org.constellation.primitives.Block
import org.constellation.state.ChainStateManager.AddBlock
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
  new ConstellationNode(keyPair, None, config.getString("http.interface"), config.getInt("http.port"), rpcTimeout)
}

class ConstellationNode(
               val keyPair: KeyPair,
               val seedPeers: Option[Seq[String]],
               val httpInterface: String,
               val httpPort: Int,
               timeoutSeconds: Int = 30
             )(
               implicit val system: ActorSystem,
               implicit val materialize: ActorMaterializer,
               implicit val executionContext: ExecutionContextExecutor
             ){

  val publicKeyHash = keyPair.getPublic.hashCode()

  val logger = Logger(s"ConstellationNode_$publicKeyHash")

  implicit val timeout: Timeout = Timeout(timeoutSeconds, TimeUnit.SECONDS)

  // TODO: add root actor for routing

  // TODO: in our p2p connection we need a blacklist of connections,
  // and need to make sure someone can't access internal actors

  // Setup actors
  val memPoolManagerActor: ActorRef =
    system.actorOf(Props(new MemPoolManager), s"ConstellationMemPoolManagerActor_$publicKeyHash")

  val chainStateActor: ActorRef =
    system.actorOf(Props(new ChainStateManager), s"ConstellationChainStateActor_$publicKeyHash")

  val consensusActor: ActorRef = system.actorOf(
    Props(new Consensus(memPoolManagerActor, chainStateActor, keyPair)(timeout)),
    s"ConstellationConsensusActor_$publicKeyHash")

  val peerToPeerActor: ActorRef =
    system.actorOf(Props(new PeerToPeer(keyPair.getPublic, system, consensusActor)(timeout)), s"ConstellationP2PActor_$publicKeyHash")

  // Seed peers
  if (seedPeers.isDefined) {
    seedPeers.get.foreach(peer => {
      logger.info(s"Attempting to connect to seed-host $peer")
      peerToPeerActor ! AddPeer(peer)
    })
  } else {
    logger.info("No seed host configured, waiting for messages.")
  }

  // If we are exposing rpc then create routes
  val routes: Route = new RPCInterface(chainStateActor,
    peerToPeerActor, memPoolManagerActor, consensusActor)(executionContext, timeout).routes

  // Setup http server for rpc
  Http().bindAndHandle(routes, httpInterface, httpPort)
}
