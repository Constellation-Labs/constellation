package org.constellation.cluster
import java.util.concurrent.TimeUnit

import akka.util.Timeout
import org.constellation.primitives._
import org.constellation.util.APIClient
import org.constellation.util.Simulation.{awaitCheckpointsAccepted, checkSnapshot}
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

  val channelOpenResponse = testApp.deploy(schemaStr, testChannelName)

  val broadcast: Future[ChannelSendResponse] = channelOpenResponse.flatMap { resp =>
    val r = resp.get
    val messages = Seq.fill(2) { SensorData.generateRandomValidMessage() }
    constellationAppSim.sim.logger.info(s"Broadcasted channel msg $resp")
    testApp.broadcast[SensorData](messages.take(1), r.channelId)
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

//  "Checkpoints" should "get accepted" in {
//    assert(constellationAppSim.sim.awaitCheckpointsAccepted(apis))
//  }

//  "Snapshots" should "get accepted" in {
//    assert(constellationAppSim.sim.checkSnapshot(apis))
//  }

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
      val channel = testApp.channelNameToId(testChannelName)
      assert(constellationAppSim.messagesReceived(channel.channelId, apis))
    }
  }

  "Broadcasted channel data" should "be accepted into all peers' snapshots" in {
    //todo should take sucessful res of snapshot created begin checking for new inclusion
    val channel = testApp.channelNameToId(testChannelName)
    assert(awaitCheckpointsAccepted(apis))
    assert(checkSnapshot(apis, num = 2))
    val channelProof = apis.head.getBlocking[Option[ChannelProof]]("channel/" + channel.channelId, timeout = 90.seconds)
    assert(channelProof.nonEmpty)

//    broadcast.map { resp =>
//      constellationAppSim.sim.logger.info(s"Broadcasted channel msg $resp")
//      constellationAppSim.sim.logger.info(s"messageHashes ${resp.messageHashes}")
//      println(s"channelIdToChannelTest $channel")//message.signedMessageData.data.channelId: testChannelName db60de054f0e450629928776f482aacfb53bd3beb6dc4ed92fbdc8061488691c
////      val test = apis.forall { a =>
////        val res = a.getBlocking[Option[ChannelProof]]("channel/" + channel.channelId, timeout = 120.seconds)
////        res.nonEmpty
////      }
////      logger.info(s"Broadcasted channel data test: ${test}")
////      Thread.sleep(20 * 1000)
//
////      assert(constellationAppSim.messagesInSnapshots(channel.channelId, apis))
//      assert(resp.errorMessage == "Success")
//    }
  }

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
