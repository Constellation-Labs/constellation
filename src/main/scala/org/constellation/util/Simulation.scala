package org.constellation.util

import java.util.concurrent.ForkJoinPool

import akka.http.scaladsl.model.StatusCodes
import org.constellation.primitives.Schema._
import constellation._

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.Random

class Simulation(apis: Seq[APIClient]) {

  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(new ForkJoinPool(100))


  def healthy(): Boolean = apis.forall{ a => a.getBlockingStr[String]("health", timeout = 100) == "OK"}
  def setIdLocal(): Unit = apis.foreach{ a =>
    val id = a.getBlocking[Id]("id")
    a.id = id
  }
  def setExternalIP(): Boolean =
    apis.forall{a => a.postSync("ip", a.host + ":" + a.udpPort).status == StatusCodes.OK}

  def verifyGenesisReceived(): Boolean = {
    apis.forall { a =>
      val gbmd = a.getBlocking[Metrics]("metrics")
      gbmd.metrics("numValidBundles").toInt == 1
    }
  }

  def genesis(): Unit = {
    val r1 = apis.head
    // Create a genesis transaction
    val numCoinsInitial = 4e9.toLong
    val genTx = r1.getBlocking[TX]("genesis/" + numCoinsInitial)
    Thread.sleep(2000)
    val gbmd = r1.getBlocking[Metrics]("metrics")
    assert(gbmd.metrics("numValidBundles").toInt == 1)
    // JSON parsing error, needs to be fixed
    /*
    val gbmd = r1.getBlocking[Seq[BundleMetaData]]("bundles")
    assert(gbmd.size == 1)
    assert(gbmd.head.height.get == 0)
    val genesisBundle = gbmd.head.bundle
    assert(genesisBundle.extractIds.head == r1.id)
    assert(genesisBundle.extractTXHash.size == 1)
    assert(gbmd.head.totalScore.nonEmpty)*/
  }

  def addPeers(): Seq[Future[Unit]] = {
    val results = apis.flatMap { a =>
      val ip = a.host
      println(s"Trying to add nodes to $ip")
      val others = apis.filter {_.id != a.id}.map { z => z.host + ":" + z.udpPort}
      others.map {
        n =>
          Future {
            val res = a.postSync("peer", n)
            println(s"Tried to add peer $n to $ip res: $res")
          }
      }
    }
    results
  }

  def verifyPeersAdded(): Boolean = apis.forall { api =>
    val peers = api.getBlocking[Seq[Peer]]("peerids")
    println("Peers length: " + peers.length)
    peers.length == (apis.length - 1)
  }

  def assignReputations(): Unit = apis.foreach{ api =>
    val others = apis.filter{_ != api}
    val havePublic = Random.nextDouble() > 0.5
    val haveSecret = Random.nextDouble() > 0.5 || havePublic
    api.postSync("reputation", others.map{o =>
      UpdateReputation(
        o.id,
        if (haveSecret) Some(Random.nextDouble()) else None,
        if (havePublic) Some(Random.nextDouble()) else None
      )
    })
  }

  def initialDistributionTX(): Seq[TX] = {

    println("-"*10)
    println("Initial distribution")
    println("-"*10)

    apis.tail.map{ n =>
      val dst = n.id.address.address
      val s = SendToAddress(dst, 1e7.toLong)
      apis.head.postRead[TX]("sendToAddress", s)
    }
  }


  def randomNode = apis(Random.nextInt(apis.length))
  def randomOtherNode(not: APIClient): APIClient = apis.filter{_ != not}(Random.nextInt(apis.length - 1))

  def sendRandomTransaction: Future[TX] = {
    Future {
      val src = randomNode
      val dst = randomOtherNode(src).id.address.address
      val s = SendToAddress(dst, Random.nextInt(1000).toLong)
      src.postRead[TX]("sendToAddress", s)
    }(ec)
  }

  def sendRandomTransactions(numTX: Int = 200): Set[TX] = {

    val numTX = 200

    val txResponse = Seq.fill(numTX) {
      Thread.sleep(1000)
      sendRandomTransaction
    }

    val txResponseFut = Future.sequence(txResponse)
    val txs = txResponseFut.get(100).toSet
    txs
  }

  def validateRun(txSent: Set[TX]): Boolean = {

    var done = false
    var attempts = 0

    while (!done && attempts < 10) {

      attempts += 1
      Thread.sleep(5000)
      val validTXs = apis.map{_.getBlocking[Seq[String]]("validTX")}
      println("Num valid TX hashes " + validTXs.map{_.size})

    }

    done
  }

  def run(): Unit = {

    assert(healthy())
    setIdLocal()
  //  assert(setExternalIP())
    genesis()

    val results = addPeers()
    import scala.concurrent.duration._
    Await.result(Future.sequence(results), 60.seconds)

    assert(verifyPeersAdded())

    Thread.sleep(3000)

    assert(verifyGenesisReceived())

    val distrTX = initialDistributionTX()

    Thread.sleep(15000)

    val start = System.currentTimeMillis()

    val txs = sendRandomTransactions()

    assert(validateRun(txs))

    val end = System.currentTimeMillis()

    println(s"Completion time seconds: ${(end-start) / 1000}")

  }

}
