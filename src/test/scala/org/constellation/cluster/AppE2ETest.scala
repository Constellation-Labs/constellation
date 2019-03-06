package org.constellation.cluster
import org.constellation.primitives.SensorData
import org.constellation.{Channel, ConstellationApp, E2E}
import org.constellation.util.{APIClient, Simulation}

class AppE2ETest extends E2E {
  val numMessages = 5
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
  val broadcast = deployResp.flatMap { resp =>
    val r = resp.get
    val messages = constellationAppSim.generateChannelMessages(r, numMessages)
    testApp.broadcast[SensorData](messages)
  }

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

  "Deployed state channels" should "get registered by peers" in {
    deployResp.map { resp: Option[Channel] =>
    val registered = resp.map(r => constellationAppSim.assertGenesisAccepted(apis)(r))
      assert(registered.contains(true))
    }
  }

  "Channel broadcasts" should "get registered by peers" in {
    broadcast.map { resp =>
      assert(resp.errorMessage == "Success")
      assert(resp.messageHashes.distinct.size == numMessages * 2)
    }
  }
}
