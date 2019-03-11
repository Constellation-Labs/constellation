package org.constellation.util

import java.security.KeyPair
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import scala.concurrent.ExecutionContext
import scala.util.Try

import org.constellation.crypto.KeyUtils
import org.constellation.{ConstellationNode, HostPort, NodeInitializationConfig}

object TestNode {

  private var nodes = Seq[ConstellationNode]()

  def apply(seedHosts: Seq[HostPort] = Seq(),
            keyPair: KeyPair = KeyUtils.makeKeyPair(),
            randomizePorts: Boolean = true,
            portOffset: Int = 0,
            isGenesisNode: Boolean = false)(
    implicit system: ActorSystem,
    materializer: ActorMaterializer,
    executionContext: ExecutionContext
  ): ConstellationNode = {

    val randomPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9000 + portOffset
    val randomPeerPort =
      if (randomizePorts) scala.util.Random.nextInt(50000) + 5000 else 9001 + portOffset

    val node = new ConstellationNode(
      NodeInitializationConfig(
        seeds = seedHosts,
        metricIntervalSeconds =  10,
        isGenesisNode = isGenesisNode,
        httpPort = randomPort,
        peerHttpPort = randomPeerPort,
        attemptDownload = seedHosts.nonEmpty,
        allowLocalhostPeers = true
      )
    )

    nodes = nodes :+ node

    node.dao.processingConfig = node.dao.processingConfig.copy(
      numFacilitatorPeers = 2,
      minCheckpointFormationThreshold = 3,
      randomTXPerRoundPerPeer = 2,
      metricCheckInterval = 10,
      maxWidth = 4,
      maxMemPoolSize = 15,
      minPeerTimeAddedSeconds = 1,
      snapshotInterval = 2,
      snapshotHeightInterval = 2,
      snapshotHeightDelayInterval = 1,
      roundsPerMessage = 1
    )

    node
  }

  def clearNodes(): Unit = {
    Try {
      nodes.foreach { node =>
        node.shutdown()
      }
      nodes = Seq()
    }
  }

}
