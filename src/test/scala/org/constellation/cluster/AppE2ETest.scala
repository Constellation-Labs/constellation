package org.constellation.cluster
import org.constellation.primitives.SensorData
import org.constellation.{Channel, ConstellationApp, E2E}
import org.constellation.util.{APIClient, Simulation}

class AppE2ETest extends E2E {
  val totalNumNodes = 3
  private val n1 = createNode(randomizePorts = false)
  private val nodes = Seq(n1) ++ Seq.tabulate(totalNumNodes - 1)(
    i => createNode(seedHosts = Seq(), randomizePorts = false, portOffset = (i * 2) + 2)
  )
  private val apis: Seq[APIClient] = nodes.map { _.getAPIClient() }
  private val addPeerRequests = nodes.map { _.getAddPeerRequest }
  private val sim = new Simulation()
  val testApp = new ConstellationApp(apis.head)
  val constellationAppSim = new ConstellationAppSim(sim, testApp)

  private val schemaStr = SensorData.jsonSchema
  val testChannelName = "testChannelName"

  val healthCheck = sim.checkHealthy(apis)
  sim.setIdLocal(apis)
  sim.addPeersFromRequest(apis, addPeerRequests)

  val goe = sim.genesis(apis)
  apis.foreach { _.post("genesis/accept", goe) }

  sim.triggerRandom(apis)
  sim.setReady(apis)

  val deployResp = testApp.deploy(schemaStr)


  "API health check" should "return true" in {
    assert(healthCheck)
  }

  "Peer health check" should "return true" in {
    assert(sim.checkPeersHealthy(apis))
  }

  "Genesis observation" should "be accepted by all nodes" in {
    assert(sim.checkGenesis(apis))
  }

  "Checkpoints" should "get accepted" in {
    assert(sim.awaitCheckpointsAccepted(apis))
  }

  "ConstellationApp" should "register a deployed state channel" in {
    deployResp.map { resp: Option[Channel] =>
      sim.logger.info("deploy response:" + resp.toString)
      assert(resp.exists(r => r.channelId == r.channelOpenRequest.genesisHash))
      assert(resp.exists(_.channelName == "channel_1"))
      assert(resp.forall(r => testApp.channelIdToChannel.get(r.channelId).contains(r)))
    }
  }
}
