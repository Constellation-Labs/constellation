package org.constellation.util

import java.net.InetSocketAddress
import java.security.KeyPair

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.constellation.ConstellationNode
import org.constellation.crypto.KeyUtils

import scala.concurrent.ExecutionContext
import scala.util.Try

object TestNode {

  private var nodes = Seq[ConstellationNode]()

  def apply(seedHosts: Seq[InetSocketAddress] = Seq(),
            keyPair: KeyPair = KeyUtils.makeKeyPair(),
            randomizePorts: Boolean = true)(
    implicit system: ActorSystem,
    materializer: ActorMaterializer,
    executionContext: ExecutionContext
  ): ConstellationNode = {

    val randomPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9000
    val randomPeerPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9001
    val randomPeerTCPPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9002
    val randomUDPPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 16180

    val node = new ConstellationNode(
      keyPair,
      seedHosts,
      "0.0.0.0",
      randomPort,
      udpPort = randomUDPPort,
      autoSetExternalAddress = true,
      peerHttpPort = randomPeerPort,
      peerTCPPort = randomPeerTCPPort,
    )

    nodes = nodes :+ node

    node.dao.processingConfig = node.dao.processingConfig.copy(
      minCBSignatureThreshold = 3,
      minCheckpointFormationThreshold = 2,
      randomTXPerRound = 10
    )
    node.dao.snapshotInterval = 5

    node
  }

  def clearNodes(): Unit = {
    Try {
      nodes.foreach { node => node.shutdown() }
      nodes = Seq()
    }
  }

}
