package org.constellation.primitives

import java.util.Collections

import io.prometheus.client.CollectorRegistry
import org.constellation.domain.configuration.NodeConfig
import org.constellation.{DAO, TestHelpers}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class MetricsManagerTest() extends Matchers with FlatSpecLike with BeforeAndAfterAll {

  implicit val dao: DAO =
    TestHelpers.prepareRealDao(nodeConfig = NodeConfig(allowLocalhostPeers = true, hostName = "", peerHttpPort = 0))

  "MetricsManager" should "report micrometer metrics" in {
    val familySamples = Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples())
    familySamples.size() should be > 0
  }
}
