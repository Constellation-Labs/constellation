package org.constellation.cluster

import java.io.File
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import constellation._
import org.constellation.p2p.PeerToPeer.{Id, Peer}
import org.constellation.primitives.Schema._
import org.constellation.primitives.{Block, Transaction}
import org.constellation.utils.TestNode
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

class MultiNodeDAGTest extends TestKit(ActorSystem("TestConstellationActorSystem")) with AsyncFlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit override val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  "E2E Multiple Nodes DAG" should "add peers and build DAG with transactions" in {

    // Cleanup DBs
    import scala.tools.nsc.io.{File => SFile}
    val tmpDir = new File("tmp")
    Try{SFile(tmpDir).deleteRecursively()}


    val nodes = Seq(TestNode(heartbeatEnabled = true, randomizePorts = false)) ++
      Seq.fill(3)(TestNode(heartbeatEnabled = true))

    for (node <- nodes) {
      assert(node.healthy)
    }

    Thread.sleep(500)

    for (node <- nodes) {
      //  println(s"Trying to add nodes to $n1")
      val others = nodes.filter{_ != node}
      others.foreach{
        n =>
          Future {
            val response = node.add(n)
            //  println(s"Trying to add $n to $n1 res: ${response}")
          }
      }
    }

    // Block here instead of sleep on result of these futures ^ TODO: Fix

    Thread.sleep(8000)

    for (node <- nodes) {
      val peers = node.rpc.getBlocking[Seq[Peer]]("peerids")
      println(s"Peers length: ${peers.length}")
      assert(peers.length == (nodes.length - 1))
    }

    val n1 = nodes.head
    val r1 = nodes.head.rpc
    // Create a genesis transaction

    val numCoinsInitial = 4e9.toLong
    val numCoinsInitialActual = 4e9.toLong * NormalizationFactor
    val tx = r1.getBlocking[TX]("genesis/" + numCoinsInitial)
    assert(tx.valid)

    Thread.sleep(2000)
/*

    for (node <- nodes) {
      val lkup = node.rpc.postRead[Option[TX]]("db", tx.hash)
      assert(lkup.get == tx)
    }

    val cache = r1.getBlocking[Map[String, TX]]("walletAddressInfo")
    val genSrc = tx.tx.data.src.head
    assert(cache(genSrc.address) == tx)
    val genDst = tx.tx.data.dst
    assert(cache(genDst.address) == tx)

   // cache.foreach(println)
    assert(genSrc.normalizedBalance == (-1 * numCoinsInitial))
    assert(genDst.normalizedBalance == numCoinsInitial)

    val filteredCache = cache.flatMap{ case (k,v) => v.output(k)}

    assert(filteredCache.size == 1)
    assert(filteredCache.head.normalizedBalance == numCoinsInitial)

    val b1 = r1.getBlocking[Seq[Address]]("balances")
    import akka.pattern.ask

 //   println(b1)
    def n1UTXO = (n1.chainStateActor ? GetUTXO).mapTo[Map[String, Long]].get()
   // assert(n1UTXO == Map(tx.tx.data.dst.address -> numCoinsInitialActual, tx.tx.data.src.head.address -> 0L))


    val n2 = nodes(1)
    val a = n2.rpc.getBlocking[Address]("address")
    val id = n2.rpc.getBlocking[Id]("id")
    val amount2 = 1e6.toLong

    val s = SendToAddress(a, amount2, Some(id.id))
    r1.post("sendToAddress", s)

    Thread.sleep(2000)

    val b = n2.rpc.getBlocking[Seq[Address]]("balances")
  //  println("num balances " + b.size)
  //  println("balances n2 " + b)
    assert(b.size == 1)
    assert(b.head.normalizedBalance == amount2)

    def n2UTXO = (n2.chainStateActor ? GetUTXO).mapTo[Map[String, Long]].get()
  //  println(n1UTXO)
  //  println(n2UTXO)


    val n3 = nodes(2)
    val a3 = n3.rpc.getBlocking[Address]("address")
    val id3 = n3.rpc.getBlocking[Id]("id")
    val s3 = SendToAddress(a3, 1.toLong, Some(id3.id))

    n2.rpc.post("sendToAddress", s3)

    Thread.sleep(2000)

    val b3 = n3.rpc.getBlocking[Seq[Address]]("balances")
    val b22 = n2.rpc.getBlocking[Seq[Address]]("balances")
    println(b22)
    println(b3)
    println(n2UTXO)
*/


    // println(b3)


    /*
      nodes.tail.foreach{ n =>
        val a = n.rpc.getBlocking[Address]("address")
        val id = n.rpc.getBlocking[Id]("id")
        val s = SendToAddress(a, 1e6.toLong, Some(id.id))
        r1.post("sendToAddress", s)
        Thread.sleep(500)
      }


  */



  //  Thread.sleep(10000)

    // Cleanup DBs
    import scala.tools.nsc.io.{File => SFile}
    Try{SFile(tmpDir).deleteRecursively()}


    assert(true)
  }

}
