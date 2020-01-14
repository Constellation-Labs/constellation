package org.constellation.primitives

import java.util.Collections

import io.prometheus.client.CollectorRegistry
import org.constellation.domain.configuration.NodeConfig
import org.constellation.{DAO, TestHelpers}
import org.scalatest.{BeforeAndAfter, FlatSpecLike, Matchers}

class MetricsManagerTest() extends Matchers with FlatSpecLike with BeforeAndAfter {

  implicit var dao: DAO = _

  before {
    dao =
      TestHelpers.prepareRealDao(nodeConfig = NodeConfig(allowLocalhostPeers = true, hostName = "", peerHttpPort = 0))
  }

  after {
    dao.unsafeShutdown()
  }

  "MetricsManager" should "report micrometer metrics" in {
    val familySamples = Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples())
    familySamples.size() should be > 0
  }
}
