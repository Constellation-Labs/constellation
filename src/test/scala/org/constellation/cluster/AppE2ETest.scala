//package org.constellation.cluster
//import java.util.concurrent.TimeUnit
//
//import akka.util.Timeout
//import org.constellation.primitives.{ChannelProof, ChannelSendResponse, SensorData}
//import org.constellation.util.APIClient
//import org.constellation.{Channel, ConstellationApp, E2E}
//import org.constellation.util.{APIClient, Simulation}
//
//import scala.concurrent.Future
//import scala.concurrent.duration._
//
//class AppE2ETest extends E2E {
//  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)
//
//  val totalNumNodes = 3
//  val n1 = createNode(randomizePorts = false)
//  val nodes = Seq(n1) ++ Seq.tabulate(totalNumNodes - 1)(
//    i => createNode(seedHosts = Seq(), randomizePorts = false, portOffset = (i * 2) + 2)
//  )
//  val apis: Seq[APIClient] = nodes.map { _.getAPIClient() }
//  val addPeerRequests = nodes.map { _.getAddPeerRequest }
//  val sim = Simulation
//
//  val numMessages = 5
//  val testNode = apis.head
//  val testApp = new ConstellationApp(testNode)
//  val constellationAppSim = new ConstellationAppSim(sim, testApp)
//
//  val schemaStr = SensorData.jsonSchema
//  val testChannelName = "testChannelName"
//
//
//  "API health check" should "return true" in {
//    val healthCheck = sim.checkHealthy(apis)
//    sim.setIdLocal(apis)
//    assert(healthCheck)
//  }
//
//  "Peer health check" should "return true" in {
//    sim.addPeersFromRequest(apis, addPeerRequests)
//    assert(sim.checkPeersHealthy(apis))
//  }
//
//  "Genesis observation" should "be accepted by all nodes" in {
//    val goe = sim.genesis(apis)
//    apis.foreach { _.post("genesis/accept", goe) }
//    assert(sim.checkGenesis(apis))
//  }
//
//  "Checkpoints" should "get accepted" in {
//    sim.triggerRandom(apis)
//    sim.setReady(apis)
//    assert(sim.awaitCheckpointsAccepted(apis))
//  }
//
//  "Snapshots" should "get accepted" in {
//    assert(sim.checkSnapshot(apis))
//  }
//
//
//  var deployResp: Future[Option[Channel]] = null
//  var broadcast: Future[ChannelSendResponse] = null
//
//  "ConstellationApp" should "register a deployed state channel" in {
//    deployResp = testApp.deploy(schemaStr, testChannelName)
//    deployResp.map { resp: Option[Channel] =>
//      sim.logger.info("deploy response:" + resp.toString)
//      assert(resp.exists(r => r.channelId == r.channelOpenRequest.genesisHash))
//      assert(resp.exists(_.name == testChannelName))
//      assert(resp.forall(r => testApp.channelNameToId.get(r.name).contains(r)))
//    }
//  }
//
//  "Deployed state channels" should "get registered by peers" in {
//    deployResp.map { resp: Option[Channel] =>
//    val registered = resp.map(r => constellationAppSim.assertGenesisAccepted(apis)(r))
//      assert(registered.contains(true))
//    }
//  }
//
//  "Channel broadcasts" should "successfully broadcast all messages" in {
//    broadcast = deployResp.flatMap { resp =>
//      val r = resp.get
//      val messages = constellationAppSim.generateChannelMessages(r, numMessages)
//      testApp.broadcast[SensorData](messages)
//    }
//    broadcast.map { resp =>
//      assert(resp.errorMessage == "Success")
//      // assert(resp.messageHashes.distinct.size == numMessages * 2)
//      assert(constellationAppSim.messagesReceived(resp.channelId, apis))
//    }
//  }
//
//  "Broadcasted channel data" should "be accepted into all peers' snapshots" in {
//    //todo should take sucessful res of snapshot created begin checking for new inclusion
//    broadcast.map { resp: ChannelSendResponse =>
//    val msg = resp.channelId
//      sim.logger.info(s"Broadcasted channel msg $msg")
//      sim.logger.info(s"messageHashes ${resp.messageHashes}")
//      val channelIdToChannelTest = testApp.channelNameToId(testChannelName)
//      println(s"channelIdToChannelTest $channelIdToChannelTest")
//      assert(constellationAppSim.messagesInSnapshots(msg, apis))
//    }
//  }
//
////  "Broadcasted channel data" should "be accepted into snapshots" in {
////    shapshotAcceptance.map { snapshottedMessages =>
////      val allMessagesValid = snapshottedMessages.forall(constellationAppSim.messagesValid)
////      assert(snapshottedMessages.nonEmpty)
////      assert(allMessagesValid)
////    }
////  }
//
//  // deployResponse.foreach{ res => res.foreach(constellationAppSim.postDownload(apis.head, _))}
//
//  // messageSim.postDownload(apis.head)
//
//  // constellationAppSim.dumpJson(storedSnapshots)
//
//}
