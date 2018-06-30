package org.constellation.cluster

import java.io.File
import java.util.concurrent.{ForkJoinPool, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import constellation._
import org.constellation.ConstellationNode
import org.constellation.primitives.Schema._
import org.constellation.util.TestNode
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Random, Try}


class MultiNodeDAGTest extends TestKit(ActorSystem("TestConstellationActorSystem"))
  with AsyncFlatSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit override val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

  "E2E Multiple Nodes DAG" should "add peers and build DAG with transactions" in {

    val totalNumNodes = 3

    // Cleanup DBs
    import scala.tools.nsc.io.{File => SFile}
    val tmpDir = new File("tmp")
    Try{SFile(tmpDir).deleteRecursively()}

    val n1 = TestNode(heartbeatEnabled = true, randomizePorts = false)

    val r1 = n1.api

    val nodes = Seq(n1) ++ Seq.fill(totalNumNodes-1)(TestNode(heartbeatEnabled = true))

    for (node <- nodes) {
      assert(node.healthy)
    }

    // Create a genesis transaction
    val numCoinsInitial = 4e9.toLong
    val genTx = r1.getBlocking[TX]("genesis/" + numCoinsInitial)
    Thread.sleep(2000)


    val results = nodes.flatMap{ node =>
      val others = nodes.filter{_ != node}
      others.map{
        n =>
          Future {
            node.api.postSync("peer", n.udpAddressString)
          }
      }
    }

    import scala.concurrent.duration._
    Await.result(Future.sequence(results), 30.seconds)

    for (node <- nodes) {
      val peers = node.api.getBlocking[Seq[Peer]]("peerids")
      assert(peers.length == (nodes.length - 1))
      val others = nodes.filter{_ != node}
      val havePublic = Random.nextDouble() > 0.5
      val haveSecret = Random.nextDouble() > 0.5 || havePublic
      node.api.postSync("reputation", others.map{o =>
        UpdateReputation(
          o.data.id,
          if (haveSecret) Some(Random.nextDouble()) else None,
          if (havePublic) Some(Random.nextDouble()) else None
        )
      })
    }



    Thread.sleep(3000)

    println("-"*10)
    println("Initial distribution")
    println("-"*10)

    val initialDistrTX = nodes.tail.map{ n =>
      val dst = n.data.selfAddress
      val s = SendToAddress(dst.address, 1e7.toLong)
      r1.postRead[TX]("sendToAddress", s)
    }

    Thread.sleep(15000)


    def randomNode: ConstellationNode = nodes(Random.nextInt(nodes.length))
    def randomOtherNode(not: ConstellationNode): ConstellationNode =
      nodes.filter{_ != not}(Random.nextInt(nodes.length - 1))

    val ec = ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    def sendRandomTransaction = {
      Future {
        val src = randomNode
        val dst = randomOtherNode(src)
        val s = SendToAddress(dst.data.id.address.address, Random.nextInt(1000).toLong)
        src.api.postRead[TX]("sendToAddress", s)
      }(ec)
    }





    val numTX = 200

    val start = System.currentTimeMillis()

    val txResponse = Seq.fill(numTX) {
      Thread.sleep(1000)
      sendRandomTransaction
    }

    val txResponseFut = Future.sequence(txResponse)


    val txs = txResponseFut.get(100).toSet

    val allTX = Set(genTx) ++ initialDistrTX.toSet ++ txs

    var done = false






    /*


        while (!done) {
          val nodeStatus = nodes.map { n =>
            Try{(n.peerToPeerActor ? GetValidTX).mapTo[Set[TX]].get()}.map { validTX =>
              val percentComplete = 100 - (allTX.diff(validTX).size.toDouble / allTX.size.toDouble) * 100
              println(s"Node ${n.data.id.short} validTXSize: ${validTX.size} allTXSize: ${allTX.size} % complete: $percentComplete")
              Thread.sleep(1000)
              validTX == allTX
            }.getOrElse(false)
          }

          if (nodeStatus.forall { x => x }) {
            done = true
          }
        }

        val end = System.currentTimeMillis()

        println(s"Completion time seconds: ${(end-start) / 1000}")

    */

    Thread.sleep(55555000)

    //  Thread.sleep(3000000)

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
        assert(genSrc.normalizedBalance == (-1 * numCoinsInitial))
        assert(genDst.normalizedBalance == numCoinsInitial)
        val filteredCache = cache.flatMap{ case (k,v) => v.output(k)}
        assert(filteredCache.size == 1)
        assert(filteredCache.head.normalizedBalance == numCoinsInitial)
    */

    /*


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
       // println(n2UTXO)

    */


    // println(b3)


    /*



  */



    //Thread.sleep(1000000)
    // Thread.sleep(1000000)

    // Cleanup DBs
    import scala.tools.nsc.io.{File => SFile}
    Try{SFile(tmpDir).deleteRecursively()}


    assert(true)
  }

}
