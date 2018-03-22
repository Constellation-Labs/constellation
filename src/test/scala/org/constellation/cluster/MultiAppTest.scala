package org.constellation.cluster


import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.constellation.app.TestNode
import org.constellation.p2p.PeerToPeer.{AddPeer, GetBalance, GetPeers, Peers}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import akka.pattern.ask
import akka.util.Timeout
import org.constellation.primitives.Transaction
import org.constellation.rpc.RPCClient

import scala.concurrent.{Await, ExecutionContextExecutor}
import org.constellation.wallet.KeyUtils._

import scala.util.Random

class MultiAppTest extends FlatSpec with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("BlockChain")
  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  "Multiple Apps" should "create and run multiple nodes within the same JVM" in {

    (1 to 3).foreach { _ =>
      TestNode.apply()
    }
    // Need for verifications as in SingleAppTest, i.e. health checks

  }

  "Multiple Connected Apps" should "create and run multiple nodes and talk to each other" in {

    val seedHostNode = TestNode.apply()

    implicit val timeout: Timeout = seedHostNode.timeout

    val seedHostPath = seedHostNode.blockChainActor.path.toSerializationFormat

    val nodes = (1 to 3).map { _ =>
      TestNode.apply(seedHostPath)
    }

    nodes.foreach{ n =>
      import scala.concurrent.duration._
      val response = Await.result(n.blockChainActor ? GetPeers, 5.seconds).asInstanceOf[Peers]
      assert(response.peers.nonEmpty)
    }

    // Need to verify nodes are healthy

  }

  "Simulation" should "run a simple transaction simulation starting from genesis" in {


    val kp0 = makeKeyPair()
    val kp1 = makeKeyPair()

    import constellation._

    val genQuantity = 1e9.toLong
    val genTX = Transaction(0L, kp0.getPublic, kp1.getPublic, genQuantity)
      .senderSign(kp0.getPrivate).counterPartySign(kp1.getPrivate)

    assert(genTX.valid)

    val master = TestNode.apply()
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

  }

  override def afterAll() {
    system.terminate()
  }

}
