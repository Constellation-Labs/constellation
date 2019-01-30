package org.constellation.util

import java.security.KeyPair

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.constellation.crypto.KeyUtils
import org.constellation.{ConstellationNode, HostPort, NodeConfig}

import scala.concurrent.ExecutionContext
import scala.util.Try

object TestNode {

  private var nodes = Seq[ConstellationNode]()

  def apply(seedHosts: Seq[HostPort] = Seq(),
            keyPair: KeyPair = KeyUtils.makeKeyPair(),
            randomizePorts: Boolean = true,
            portOffset: Int = 0
           )(
    implicit system: ActorSystem,
    materializer: ActorMaterializer,
    executionContext: ExecutionContext
  ): ConstellationNode = {

    val randomPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9000 + portOffset
    val randomPeerPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9001 + portOffset
    val randomPeerTCPPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9002 + portOffset
    val randomUDPPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 16180 + portOffset

    val node = new ConstellationNode(
      keyPair,
      seedHosts,
      "0.0.0.0",
      randomPort,
      udpPort = randomUDPPort,
      autoSetExternalAddress = true,
      peerHttpPort = randomPeerPort,
      peerTCPPort = randomPeerTCPPort,
      attemptDownload = seedHosts.nonEmpty,
      allowLocalhostPeers = true,
      nodeConfig = NodeConfig(10)
    )

    nodes = nodes :+ node

    node.dao.processingConfig = node.dao.processingConfig.copy(
      numFacilitatorPeers = 2,
      minCheckpointFormationThreshold = 3,
      randomTXPerRoundPerPeer = 1,
      metricCheckInterval = 10,
      maxWidth = 3,
      maxMemPoolSize = 15,
      minPeerTimeAddedSeconds = 1,
      snapshotInterval = 5,
      snapshotHeightInterval = 2,
      snapshotHeightDelayInterval = 1,
      roundsPerMessage = 1
    )

    node
  }

  def clearNodes(): Unit = {
    Try {
      nodes.foreach { node => node.shutdown() }
      nodes = Seq()
    }
  }

}
