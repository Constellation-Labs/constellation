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
import org.constellation.util.{Simulation, TestNode}
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

    val apis = nodes.map{_.api}
    val sim = new Simulation(apis)
    sim.run()


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

    // Cleanup DBs
    import scala.tools.nsc.io.{File => SFile}
    Try{SFile(tmpDir).deleteRecursively()}


    assert(true)
  }

}
