package org.constellation.cluster
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import org.constellation.primitives._
import org.constellation.util.APIClient
import org.constellation.{Channel, ConstellationApp, E2E}
import org.constellation.util.{APIClient, Simulation}

import scala.concurrent.Future
import scala.concurrent.duration._

class AppE2ETest extends E2E {
  implicit val timeout: Timeout = Timeout(120, TimeUnit.SECONDS)

  val totalNumNodes = 3
  val n1 = createNode(randomizePorts = false)
  val nodes = Seq(n1) ++ Seq.tabulate(totalNumNodes - 1)(
    i => createNode(seedHosts = Seq(), randomizePorts = false, portOffset = (i * 2) + 2)
  )
  val apis: Seq[APIClient] = nodes.map { _.getAPIClient() }
  val addPeerRequests = nodes.map { _.getAddPeerRequest }

  val numMessages = 5
  val testNodeApi = apis.head
  val testApp = new ConstellationApp(testNodeApi)
  val constellationAppSim = new ConstellationAppSim(testApp)

  val schemaStr = SensorData.jsonSchema
  val testChannelName = "testChannelName"

  val healthCheck = constellationAppSim.sim.checkHealthy(apis)
  constellationAppSim.sim.setIdLocal(apis)
  constellationAppSim.sim.addPeersFromRequest(apis, addPeerRequests)
  val peerHealthCheck = constellationAppSim.sim.checkPeersHealthy(apis)

  val goe = constellationAppSim.sim.genesis(apis)
  apis.foreach { _.post("genesis/accept", goe) }

  constellationAppSim.sim.triggerRandom(apis)
  constellationAppSim.sim.setReady(apis)

  val channelOpenResponse = testApp.deploy(testChannelName, schemaStr)

  val broadcast: Future[ChannelSendResponse] = channelOpenResponse.flatMap { resp =>
    val r = resp.get
    val messages = constellationAppSim.generateChannelMessages(r, numMessages)
    constellationAppSim.sim.logger.info(s"Broadcasted channel msg $resp")
    testApp.broadcast[SensorData](messages, r.channelId)
  }

  "API health check" should "return true" in {
    assert(healthCheck)
  }

  "Peer health check" should "return true" in {
    assert(peerHealthCheck)
  }

  "Genesis observation" should "be accepted by all nodes" in {
    assert(constellationAppSim.sim.checkGenesis(apis))
  }

  "Checkpoints" should "get accepted" in {
    assert(constellationAppSim.sim.awaitCheckpointsAccepted(apis))
  }

  "Snapshots" should "get accepted" in {
    assert(constellationAppSim.sim.checkSnapshot(apis))
  }

  "ConstellationApp" should "register a deployed state channel" in {
    channelOpenResponse.map{ res => assert(res.exists(_.channelOpenRequest.errorMessage == "Success"))}
//    assert(channelOpenResponse.exists(_.errorMessage == "Success"))
//    response.map { resp: Option[Channel] =>
//      constellationAppSim.sim.logger.info("deploy response:" + resp.toString)
//      assert(resp.exists(r => r.channelId == r.channelOpenRequest.genesisHash))
//      assert(resp.exists(_.name == testChannelName))
//      assert(resp.forall(r => testApp.channelNameToId.get(r.name).contains(r)))
//    }
  }

  "Deployed state channels" should "get registered by peers" in {
    channelOpenResponse.map { resp: Option[Channel] =>
    val registered = resp.map(r => constellationAppSim.assertGenesisAccepted(apis)(r))
      assert(registered.contains(true))
    }
  }

  "Channel broadcasts" should "successfully broadcast all messages" in {
    broadcast.map { resp =>
      assert(resp.errorMessage == "Success")
      // assert(resp.messageHashes.distinct.size == numMessages * 2)
//      assert(constellationAppSim.messagesReceived(resp.channelId, apis))
    }
  }

//  "Broadcasted channel data" should "be accepted into all peers' snapshots" in {
//    //todo should take sucessful res of snapshot created begin checking for new inclusion
//    broadcast.map { resp =>
//      constellationAppSim.sim.logger.info(s"Broadcasted channel msg $resp")
//      constellationAppSim.sim.logger.info(s"messageHashes ${resp.messageHashes}")
//      val channelIdToChannelTest = testApp.channelNameToId(testChannelName)
//      println(s"channelIdToChannelTest $channelIdToChannelTest")
////      assert(constellationAppSim.messagesInSnapshots(resp, apis))
//      assert(resp.errorMessage == "Success")
//    }
//  }

//  "Broadcasted channel data" should "be accepted into snapshots" in {
//    shapshotAcceptance.map { snapshottedMessages =>
//      val allMessagesValid = snapshottedMessages.forall(constellationAppSim.messagesValid)
//      assert(snapshottedMessages.nonEmpty)
//      assert(allMessagesValid)
//    }
//  }

  // deployResponse.foreach{ res => res.foreach(constellationAppSim.postDownload(apis.head, _))}

  // messageSim.postDownload(apis.head)

  // constellationAppSim.dumpJson(storedSnapshots)

}
