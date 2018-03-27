package org.constellation.cluster

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContextExecutor
import constellation._
import org.constellation.Fixtures
import org.constellation.p2p.PeerToPeer.{Id, Peers}
import org.constellation.primitives.Chain.Chain
import org.constellation.primitives.Transaction
import org.constellation.utils.{RPCClient, TestNode}
import org.constellation.wallet.KeyUtils

class MultiNodeTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("TestConstellationActorSystem")
  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  "Multiple Nodes" should "create and run within the same JVM context" in {
    val node1 = TestNode()
    val node2 = TestNode()
    val node3 = TestNode()

    val rpc1 = new RPCClient(port=node1.httpPort)
    val rpc2 = new RPCClient(port=node2.httpPort)
    val rpc3 = new RPCClient(port=node3.httpPort)

    val rpc1Response = rpc1.get("health")
    val rpc2Response = rpc2.get("health")
    val rpc3Response = rpc3.get("health")

    assert(rpc1Response.get().status == StatusCodes.OK)
    assert(rpc2Response.get().status == StatusCodes.OK)
    assert(rpc3Response.get().status == StatusCodes.OK)
  }

  "Multiple Nodes" should "be able to gossip peers and join a cluster together correctly" in {
    val node1 = TestNode()
    val node2 = TestNode()
    val node3 = TestNode()
    val node4 = TestNode()

    val rpc1 = new RPCClient(port=node1.httpPort)
    val rpc2 = new RPCClient(port=node2.httpPort)
    val rpc3 = new RPCClient(port=node3.httpPort)
    val rpc4 = new RPCClient(port=node4.httpPort)

    val rpc1Response = rpc1.get("health")
    val rpc2Response = rpc2.get("health")
    val rpc3Response = rpc3.get("health")
    val rpc4Response = rpc4.get("health")

    assert(rpc1Response.get().status == StatusCodes.OK)
    assert(rpc2Response.get().status == StatusCodes.OK)
    assert(rpc3Response.get().status == StatusCodes.OK)
    assert(rpc4Response.get().status == StatusCodes.OK)

    // Connect the first two nodes
    val node1Path = node1.peerToPeerActor.path.toSerializationFormat
    val node2Path = node2.peerToPeerActor.path.toSerializationFormat
    val node3Path = node3.peerToPeerActor.path.toSerializationFormat
    val node4Path = node4.peerToPeerActor.path.toSerializationFormat

    rpc1.post("peer", node2Path)

    // TODO: find better way
    Thread.sleep(100)

    val node1PeersRequest = rpc1.get("peers")
    val node2PeersRequest = rpc2.get("peers")

    val node3PeersRequest = rpc3.get("peers")
    val node4PeersRequest = rpc4.get("peers")

    val peers1 = rpc1.read[Peers](node1PeersRequest.get()).get()
    val peers2 = rpc2.read[Peers](node2PeersRequest.get()).get()

    val peers3 = rpc3.read[Peers](node3PeersRequest.get()).get()
    val peers4 = rpc4.read[Peers](node4PeersRequest.get()).get()

    assert(peers1 == Peers(Seq(node2Path)))
    assert(peers2 == Peers(Seq(node1Path)))

    assert(peers3 == Peers(Seq()))
    assert(peers4 == Peers(Seq()))

    // Connect the second two nodes
    rpc3.post("peer", node4Path)

    // TODO: find better way
    Thread.sleep(100)

    val node1PeersRequest_2 = rpc1.get("peers")
    val node2PeersRequest_2 = rpc2.get("peers")

    val node3PeersRequest_2 = rpc3.get("peers")
    val node4PeersRequest_2 = rpc4.get("peers")

    val peers1_2 = rpc1.read[Peers](node1PeersRequest_2.get()).get()
    val peers2_2 = rpc2.read[Peers](node2PeersRequest_2.get()).get()

    val peers3_2 = rpc3.read[Peers](node3PeersRequest_2.get()).get()
    val peers4_2 = rpc4.read[Peers](node4PeersRequest_2.get()).get()

    assert(peers1_2 == Peers(Seq(node2Path)))
    assert(peers2_2 == Peers(Seq(node1Path)))

    assert(peers3_2 == Peers(Seq(node4Path)))
    assert(peers4_2 == Peers(Seq(node3Path)))

    // Connect the two pairs
    rpc2.post("peer", node3Path)

    // TODO: find better way
    Thread.sleep(100)

    val node1PeersRequest_3 = rpc1.get("peers")
    val node2PeersRequest_3 = rpc2.get("peers")

    val node3PeersRequest_3 = rpc3.get("peers")
    val node4PeersRequest_3 = rpc4.get("peers")

    val peers1_3 = rpc1.read[Peers](node1PeersRequest_3.get()).get()
    val peers2_3 = rpc2.read[Peers](node2PeersRequest_3.get()).get()

    val peers3_3 = rpc3.read[Peers](node3PeersRequest_3.get()).get()
    val peers4_3 = rpc4.read[Peers](node4PeersRequest_3.get()).get()

    assert(peers1_3 == Peers(Seq(node2Path, node3Path, node4Path)))
    assert(peers2_3 == Peers(Seq(node1Path, node3Path, node4Path)))

    assert(peers3_3 == Peers(Seq(node4Path, node2Path, node1Path)))
    assert(peers4_3 == Peers(Seq(node3Path, node2Path, node1Path)))

  }

  // TODO update
  "Simulation" should "run a simple transaction simulation starting from genesis" in {

    /*

    val kp0 = makeKeyPair()
    val kp1 = makeKeyPair()

    import constellation._

    val genQuantity = 1e9.toLong
    val genTX = Transaction(0L, kp0.getPublic, kp1.getPublic, genQuantity)
      .senderSign(kp0.getPrivate).counterPartySign(kp1.getPrivate)

    assert(genTX.valid)

    val master = TestNode()
    implicit val timeout: Timeout = master.timeout
    val mnode = master.blockChainActor
    val mrpc = new RPCClient(port = master.httpPort)

    assert(mrpc.read[Transaction](mrpc.sendTx(genTX).get()).get() == genTX)

    import akka.pattern._
    val bal = (mnode ? GetBalance(kp1.getPublic)).mapTo[Balance].get()

    assert(bal.balance == genQuantity)

    val seedHostPath = mnode.path.toSerializationFormat

    val nodes = (1 to 3).map { _ =>
      TestNode.apply(seedHostPath)
    }

    // This is here because for some reason the master wasn't connecting to one of the peers?
    // Investigate later. 3 Peers were showing 4 responses, 1 was showing 3. One peer was missing on a response.
    val allNodes = nodes :+ master

    allNodes.foreach{ n =>
      allNodes.foreach{ m =>
        n.blockChainActor ! AddPeer(m.blockChainActor.path.toSerializationFormat)
      }
    }

    mnode ! SetMaster

    nodes.foreach{ n =>
      import scala.concurrent.duration._
      val response = Await.result(n.blockChainActor ? GetPeers, 5.seconds).asInstanceOf[Peers]
      assert(response.peers.nonEmpty)
    }

    val fullChain = (mnode ? GetChain).mapTo[FullChain].get()

    assert(fullChain.blockChain.head.transactions.head == genTX)

    val nodeFullChains = nodes.map{ n => (n.blockChainActor ? GetChain).mapTo[FullChain].get()}

    assert(nodeFullChains.forall(_ == fullChain))

    val distr = Seq.fill(nodes.length){makeKeyPair()}

    val distrTX = distr.map{ d =>
      Transaction(0L, kp1.getPublic, d.getPublic, 1e7.toLong)
        .senderSign(kp0.getPrivate).counterPartySign(d.getPrivate)
    }

    distrTX.foreach{t => mnode ! t}

    def getAllFullChains = allNodes.map{ n =>
      (n.blockChainActor ? GetChain).mapTo[FullChain].get()
    }

    var i = 0
    var done = false

    val allKeys = distr ++ Seq(kp1)

    while (!done) {
      i += 1
      if (i > 20) done = true
      Thread.sleep(1000)
      val chains = getAllFullChains
      val accounts = allKeys.flatMap { k =>
        (mnode ? GetAccountDetails(k.getPublic)).mapTo[Option[AccountData]].get()
      }

      println("" + chains.map{z =>
        s"${z.blockChain.size} block height, ${z.blockChain.last.transactions.length} tx last block, " +
          s"${z.blockChain.map{_.transactions.length}.sum} tx total - accounts: $accounts"
      }.head)
      if (chains.forall{_.blockChain.length > 2}) {

        (0 until Random.nextInt(25) + 5).foreach{ txI =>

          val sender = allKeys(Random.nextInt(allKeys.length))
          val rx = allKeys(Random.nextInt(allKeys.length))
          val details = (mnode ? GetAccountDetails(sender.getPublic)).mapTo[Option[AccountData]].get()
          val sn = details.get.sequenceNumber
          val t = Transaction(sn, sender.getPublic, rx.getPublic, Random.nextInt(1e3.toInt).toLong)
            .senderSign(sender.getPrivate).counterPartySign(rx.getPrivate)
          mnode ! t


        }

      }

    }
    */

  }

  override def afterAll() {
    system.terminate()
  }

}
