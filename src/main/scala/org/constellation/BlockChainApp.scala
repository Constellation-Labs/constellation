package org.constellation

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.constellation.actor.ChainActor
import org.constellation.blockchain.Chain
import org.constellation.p2p.PeerToPeer.AddPeer
import org.constellation.rpc.RPCInterface

object BlockChainApp extends App with RPCInterface {

  implicit val system = ActorSystem("BlockChain")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val config = ConfigFactory.load()
  val logger = Logger("WebServer")

  val id = args.headOption.getOrElse("ID")
  println("id: " + id)
  val blockChainActor = system.actorOf(ChainActor.props(Chain(id)), "blockChainActor")
  val seedHost = config.getString("blockchain.seedHost")

  if ( ! seedHost.isEmpty ) {
    logger.info(s"Attempting to connect to seed-host ${seedHost}")
    blockChainActor ! AddPeer(seedHost)
  } else {
    logger.info("No seed host configured, waiting for messages.")
  }

  Http().bindAndHandle(routes, config.getString("http.interface"), config.getInt("http.port"))
}
